from scopecuisine.notifiers.interface import AbstractNotifier
import yagmail


class YagmailNotifier(AbstractNotifier):
    def __init__(self, *args, sender, password, receiver, **kwargs):
        super().__init__(*args, **kwargs)
        self.sender_email = sender
        self.password = password
        self.receiver_email = receiver

    def notify(self):
        if ("@" not in self.sender_email) or ("@" not in self.receiver_email):
            return False
        subject = f"Your {self.setup_name} experiment is complete"
        sender_password = self.password

        yag = yagmail.SMTP(user=self.sender_email, password=sender_password)

        body = [
            "Hey!",
            "\n",
            f"Your {self.setup_name} experiment has finished and was a success! Come pick up your little fish",
            "\n" "fishgitbot",
        ]
        try:
            yag.send(
                to=self.receiver_email,
                subject=subject,
                contents=body,
            )
            return True
        except (
            yagmail.error.YagAddressError,
            yagmail.error.YagConnectionClosed,
            yagmail.error.YagInvalidEmailAddress,
        ):
            return False

