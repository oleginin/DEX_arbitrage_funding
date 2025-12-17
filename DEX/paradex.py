import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv

# Імпортуємо тільки необхідне
from paradex_py import ParadexSubkey


class ParadexEngine:
    def __init__(self):
        env_path = Path('data') / '.env'
        load_dotenv(dotenv_path=env_path)

        # Ініціалізація клієнта
        # Вказуємо "prod" або "testnet" як рядок, оскільки Environment - це Literal
        self.paradex = ParadexSubkey(
            env="prod",
            l2_private_key=os.getenv("PARADEX_SUBKEY"),
            l2_address=os.getenv("PARADEX_ACCOUNT_ADDRESS")
        )

        # Створюємо постійний цикл подій для асинхронних запитів
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        # Одноразова ініціалізація акаунту
        try:
            self.loop.run_until_complete(self.paradex.init_account())
            print("✅ Paradex Engine успішно ініціалізовано на PROD")
        except Exception as e:
            print(f"❌ Помилка ініціалізації Paradex: {e}")

