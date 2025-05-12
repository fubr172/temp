async def end_match(server):
    try:
        match_collection = await get_match_collection(server)
        match = await match_collection.find_one({"server_name": server["name"], "active": True})

        match = await match_collection.find_one({
            "server_name": server["name"],
            "active": True
        })

        if not match:
            logging.warning(f"Не найдено активного матча для сервера {server['name']}")
            return False

        end_time = datetime.now(timezone.utc)

        await match_collection.update_one(
            {"_id": match["_id"]},
            {"$set": {"active": False}}
        )

        start_time = match['start_time']
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)

        await match_collection.update_one(
            {"_id": match["_id"]},
            {
                "$set": {
                    "active": False,
                    "end_time": end_time,
                }
            }
        )

        duration_minutes = (end_time - start_time).total_seconds() / 60
        logging.info(
            f"Матч завершён на сервере {server['name']}. "
            f"Продолжительность: {round(duration_minutes, 1)} мин."
        )

        await calculate_final_stats(server)
        return True

    except Exception as e:
        logging.error(f"Ошибка при завершении матча {server['name']}: {str(e)}")
        return False
