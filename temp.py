async def remove_disconnected_players(server):
    """Удаляет статистику по SteamID и игроков по EOSID с проверкой через Player"""
    try:
        server_name = server["name"]
        client = mongo_clients.get(server_name)
        if not client:
            logging.error(f"MongoDB client not found: {server_name}")
            return

        db = client[server["db_name"]]
        matches_col = db[server["matches_collection_name"]]
        players_col = db[server["collection_name"]]
        onl_stats_col = db[server["onl_stats_collection_name"]]

        # 1. Получить завершенный матч
        match = await matches_col.find_one(
            {"server_name": server_name, "active": False},
            projection={"disconnected_players": 1, "players": 1}
        )
        if not match:
            logging.warning(f"No active match: {server_name}")
            return

        # 2. Получить EOSID для удаления
        eos_to_process = match.get("disconnected_players", []).copy()
        all_steam_ids_to_remove = set()

        # 3. Удаление по EOSID из players
        if eos_to_process:
            # Найти записи в players с этими EOSID
            await matches_col.update_one(
                {"_id": match["_id"]},
                {"$pull": {"players": {"eos_id": {"$in": eos_to_process}}}}
            )

            # Найти SteamID удаленных игроков
            removed_players = await matches_col.find_one(
                {"_id": match["_id"]},
                {"players": 1}
            )

            # Собрать SteamID для удаления из onl_stats
            steam_ids_from_eos = [p["steam_id"] for p in removed_players.get("players", [])
                                  if p.get("eos_id") in eos_to_process]

            # Добавить SteamID для удаления из onl_stats
            all_steam_ids_to_remove.update(steam_ids_from_eos)

            # Убрать обработанные EOSID
            eos_to_process = [eos for eos in eos_to_process
                              if eos not in {p.get("eos_id") for p in removed_players.get("players", [])}]

            # 4. Обработка оставшихся EOSID через коллекцию Player
            if eos_to_process:
            # Найти SteamID по EOSID в Player
                players_data = await players_col.find(
                    {"eosid": {"$in": eos_to_process}},
                    {"_id": 1}
                ).to_list(length=None)

            # Собрать найденные SteamID
            additional_steam_ids = [p["_id"] for p in players_data]

            # Удалить записи из players по SteamID
            if additional_steam_ids:
                await matches_col.update_one(
                    {"_id": match["_id"]},
                    {"$pull": {"players": {"steam_id": {"$in": additional_steam_ids}}}}
                )
            # Добавить к общему списку для удаления из onl_stats
            all_steam_ids_to_remove.update(additional_steam_ids)

            # 5. Удалить из onl_stats по SteamID
            if all_steam_ids_to_remove:
                await onl_stats_col.delete_many({"_id": {"$in": list(all_steam_ids_to_remove)}})
            logging.info(f"[{server_name}] Удалено из onl_stats: {len(all_steam_ids_to_remove)}")


    except Exception as e:
        logging.error(f"Ошибка в remove_disconnected_players: {str(e)}")
