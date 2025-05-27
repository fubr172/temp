YOUR_CHANNEL_ID = 1376903047556497528
# Добавить новую команду и View для кнопки
@bot.tree.command(name="vip_button", description="Получения VIP на 3 дня")
@app_commands.describe(steam_id="Steam ID игрока")
@command_logger_decorator
async def create_vip_button(interaction: discord.Interaction, steam_id: str):
    # Проверка канала (замените YOUR_CHANNEL_ID на ID нужного канала)
    if interaction.channel_id != YOUR_CHANNEL_ID:
        await interaction.response.send_message("❌ Эту команду можно использовать только в специальном канале!",
                                                ephemeral=True)
        return

    # Проверка формата SteamID
    if not re.match(STEAMID64_REGEX, steam_id):
        await interaction.response.send_message("❌ Неверный формат SteamID!", ephemeral=True)
        return

    # Создаем сообщение с кнопкой
    view = discord.ui.View(timeout=None)
    button = OneTimeVIPButton(steam_id=steam_id)
    view.add_item(button)

    await interaction.response.send_message(
        f"🎮 Нажмите кнопку, чтобы получить VIP на 3 дня для SteamID: `{steam_id}`",
        view=view
    )


# Класс для одноразовой кнопки VIP
class OneTimeVIPButton(discord.ui.Button):
    def __init__(self, steam_id: str):
        super().__init__(
            style=discord.ButtonStyle.blurple,
            label="Получить VIP на 3 дня",
            custom_id=f"vip_button_{steam_id}"
        )
        self.steam_id = steam_id
        self.clicked_users = set()

    async def callback(self, interaction: discord.Interaction):
        # Проверка на повторное нажатие
        if interaction.user.id in self.clicked_users:
            await interaction.response.send_message("❌ Вы уже активировали VIP!", ephemeral=True)
            return
        self.clicked_users.add(interaction.user.id)

        # Выдача VIP
        try:
            # Логика выдачи VIP (адаптировано из команды add_vip)
            days = 3
            end_date = datetime.now() + timedelta(days=days)

            # Обновление данных для Steam
            vip_data_steam = []
            for file_path in VM_FILE_PATHS:
                vip_data = load_vip_data(file_path)
                vip_data_steam.extend(vip_data)

            updated = False
            for i, entry in enumerate(vip_data_steam):
                match = re.match(ENTRY_REGEX, entry)
                if match and match.group(1) == self.steam_id:
                    vip_data_steam[i] = f"Admin={self.steam_id}:VIP // {end_date.strftime('%Y-%m-%d')}"
                    updated = True
                    break

            if not updated:
                vip_data_steam.append(f"Admin={self.steam_id}:VIP // {end_date.strftime('%Y-%m-%d')}")

            # Сохранение во всех файлах
            for file_path in VM_FILE_PATHS:
                save_vip_data(vip_data_steam, file_path)

            # Делаем кнопку неактивной
            self.disabled = True
            await interaction.response.edit_message(view=self.view)

            await interaction.followup.send(
                f"✅ VIP на 3 дня выдан для SteamID: `{self.steam_id}`",
                ephemeral=True
            )

            # Логирование
            command_logger.info(
                f"Пользователь {interaction.user.name} активировал VIP для {self.steam_id}"
            )

        except Exception as e:
            logging.error(f"Ошибка выдачи VIP: {e}")
            await interaction.response.send_message("❌ Произошла ошибка!", ephemeral=True)
