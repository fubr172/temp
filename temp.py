@command_logger_decorator
@bot.tree.command(name="add_vip", description="Добавить VIP статус")
@app_commands.describe(days="Количество дней VIP", steam_id="Steam ID игрока")
async def add_vip(interaction: discord.Interaction, days: int, steam_id: str):
    for file_path in VM_FILE_PATHS + PLAYER_PREFIXES_PATH:
        logging.info(f"Вызов функции create_config_backup")
        create_config_backup(file_path)

    user = interaction.user
    command_logger.info(
        f"Команда 'add_vip' вызвана пользователем {user.name} "
        f"с параметрами: дни {days}, SteamID {steam_id}."
    )

    await interaction.response.defer(ephemeral=True)

    if interaction.channel_id != ADM_ADD_VIP:
        command_logger.info(
            f"Пользователь {user.name} пытался использовать команду не в нужном канале.")
        return await interaction.followup.send(
            f"❌ Эту команду можно использовать только в канале <#{ADM_ADD_VIP}>!",
            ephemeral=True
        )

    if not re.match(STEAMID64_REGEX, steam_id):
        command_logger.info(f"Пользователь {user.name} ввёл неверный формат SteamID: {steam_id}.")
        await interaction.followup.send(
            "❌ Неверный формат SteamID! Используйте SteamID64 (17 цифр)",
            ephemeral=True
        )
        return

    # НОВАЯ ПРОВЕРКА НА КОЛИЧЕСТВО ДНЕЙ
    if days < 1:
        command_logger.info(f"Пользователь {user.name} ввёл некорректное количество дней: {days}.")
        await interaction.followup.send(
            f"❌ Минимальное количество дней: 1",
            ephemeral=True
        )
        return

    try:
        user_data = users.find_one({"steam_id": steam_id})
        if not user_data:
            return await interaction.followup.send(
                "❌ Пользователь с таким SteamID не найден в базе данных",
                ephemeral=True
            )

        discord_id = user_data.get('discord_id')
        if not discord_id:
            return await interaction.followup.send(
                "❌ У этого пользователя не привязан Discord аккаунт",
                ephemeral=True
            )

        eos_id = get_eos_id_from_mongo(steam_id)

        # УПРОЩЕННЫЙ РАСЧЕТ ДАТЫ ОКОНЧАНИЯ
        end_date = datetime.now() + timedelta(days=days)
        new_entry_eos = f"{eos_id} = VIP // {end_date.strftime('%Y-%m-%d')}"

        vip_data_steam = []
        for file_path in VM_FILE_PATHS:
            vip_data_steam.extend(load_vip_data(file_path))

        updated_steam = False
        for i, entry in enumerate(vip_data_steam):
            match = re.match(ENTRY_REGEX, entry)
            if match and match.group(1) == steam_id:
                vip_data_steam[i] = f"Admin={steam_id}:VIP // {end_date.strftime('%Y-%m-%d')}"
                updated_steam = True
                break

        if not updated_steam:
            vip_data_steam.append(f"Admin={steam_id}:VIP // {end_date.strftime('%Y-%m-%d')}")

        vip_data_eos = []
        for file_path in PLAYER_PREFIXES_PATH:
            vip_data_eos.extend(load_vip_data(file_path))

        updated_eos = False
        for i, entry in enumerate(vip_data_eos):
            match = re.match(ENTRY_REGEX_EOS, entry)
            if match and match.group(1) == eos_id:
                vip_data_eos[i] = new_entry_eos
                updated_eos = True
                break

        if not updated_eos:
            vip_data_eos.append(new_entry_eos)

        for file_path in VM_FILE_PATHS:
            save_vip_data(vip_data_steam, file_path)

        for file_path in PLAYER_PREFIXES_PATH:
            save_vip_data(vip_data_eos, file_path)

        guild = interaction.guild
        vip_role = discord.utils.get(guild.roles, name=VIP_ROLE)
        if not vip_role:
            return await interaction.followup.send(
                f"❌ Роль '{VIP_ROLE}' не найдена на сервере!",
                ephemeral=True
            )

        member = guild.get_member(int(discord_id))
        role_message = ""
        if member:
            try:
                await member.add_roles(vip_role)
                role_message = f"✅ Роль {VIP_ROLE} выдана пользователю {member.display_name}"
            except discord.Forbidden:
                role_message = "⚠ Не удалось выдать роль (недостаточно прав)"
            except discord.HTTPException as e:
                role_message = f"⚠ Ошибка при выдаче роли: {str(e)}"
        else:
            role_message = "⚠ Пользователь не найден на сервере"

        # ОБНОВЛЕННОЕ СООБЩЕНИЕ С УКАЗАНИЕМ ДНЕЙ
        command_logger.info(f"VIP статус для SteamID {steam_id} добавлен на {days} дней.")
        await interaction.followup.send(
            f"✅ Добавлен VIP для SteamID `{steam_id}`\n"
            f"Количество дней: {days}\n"
            f"Дата окончания: {end_date.strftime('%d.%m.%Y')}\n"
            f"{role_message}",
            ephemeral=True
        )

    except Exception as e:
        logging.exception(f"Ошибка при выполнении команды add_vip: {e}")
        command_logger.error(f"Ошибка при выполнении команды add_vip: {str(e)}")
        await interaction.followup.send(
            f"⛔ Ошибка: {str(e)}",
            ephemeral=True
        )
