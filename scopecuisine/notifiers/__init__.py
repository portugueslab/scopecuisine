from scopecuisine.notifiers.yagmail import YagmailNotifier
from scopecuisine.notifiers.interface import AbstractNotifier

notifiers = dict(none=AbstractNotifier, yagmail=YagmailNotifier)
