class AbstractNotifier:
    def __init__(self, setup_name):
        self.setup_name = setup_name

    def notify(self):
        return False
