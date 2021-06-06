import os


class TelegramNotification:

    def __init__(self, chat_id, token):
        self.token = token
        self.chat_id = chat_id
        self.bot = None

        ##
        self.supports_html = None
        if os.path.isfile(token):
            with open(token, "r") as f:
                self.token = f.read()

    def send(self, message):
        try:
            import telegram

            # Create bot
            if self.bot is None:
                self.bot = telegram.Bot(self.token)

            # Check if telegram version supports html
            if self.supports_html is None and "ParseMode" in dir(telegram):
                self.supports_html = True
            else:
                self.supports_html = False

            # Send message
            if self.supports_html:
                self.bot.sendMessage(self.chat_id, text=message, parse_mode=telegram.ParseMode.HTML)
            else:
                self.bot.sendMessage(self.chat_id, text=message.replace("<b>", "").replace("</b>", ""))

            return True
        except:
            return False

    def send_attachment(self, message, file):
        try:
            import telegram
            if self.bot is None:
                self.bot = telegram.Bot(self.token)

            dfile = file
            if isinstance(file, str):
                dfile = open(file, "rb")

            self.bot.sendDocument(self.chat_id, document=dfile, caption=message)
            return True

        except:
            return False


class EmailNotification:

    def __init__(self, username, password, recipients=[]):
        self.password = password
        self.username = username
        self.recipients = recipients

        #
        if len(recipients) == 0: self.recipients = [username]

        if os.path.isfile(password):
            with open(password, "r") as f:
                self.password = f.read()

    def send(self, message):
        return