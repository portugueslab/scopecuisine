#!/usr/bin/env python
from scopecuisine.notifiers.yagmail import YagmailNotifier


def test_yagmail():
    notifier = YagmailNotifier("test", sender="", password="", receiver="")
    assert not notifier.notify()

    notifier = YagmailNotifier("test", sender="a@b.com", password="", receiver="")
    assert not notifier.notify()

