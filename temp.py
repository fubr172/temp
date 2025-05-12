async def end_match(server):
    try:
        match_collection = await get_match_collection(server)
        
        # Ищем активный матч (убрано дублирование find_one)
        match = await match_collection.find_one({
            "server_name": server["name"],
            "active": True
        })

        if not match:
            logging.warning(f"Активный матч не найден: {server['name']}")
            return False

        # Проверяем, не завершен ли уже матч
        if not match.get("active", True):
            logging.warning(f"Матч {match['_id']} уже завершен")
            return False

        end_time = datetime.now(timezone.utc)
        start_time = match["start_time"].replace(tzinfo=timezone.utc)

        # Обновляем матч и проверяем результат
        result = await match_collection.update_one(
            {"_id": match["_id"], "active": True},  # Добавлено условие для атомарности
            {
                "$set": {
                    "active": False,
                    "end_time": end_time
                }
            }
        )

        # Если не было изменений, выходим
        if result.modified_count == 0:
            logging.warning(f"Матч {match['_id']} уже был завершен")
            return False

        # Логируем и генерируем статистику
        duration = (end_time - start_time).total_seconds() / 60
        logging.info(f"Матч завершен: {server['name']} ({round(duration, 1)} мин.)")
        
        # Добавляем защиту от повторного вызова
        if not hasattr(end_match, "processed"):
            end_match.processed = set()
            
        if match["_id"] not in end_match.processed:
            await calculate_final_stats(server)
            end_match.processed.add(match["_id"])
            asyncio.get_event_loop().call_later(300, end_match.processed.remove, match["_id"])  # Забыть через 5 минут

        return True

    except Exception as e:
        logging.error(f"Ошибка завершения матча {server['name']}: {str(e)}", exc_info=True)
        return False
