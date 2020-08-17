import yagmail


def send_email_end(receiver_email, sender_email, sender_password):
    sender_email = str
    # TODO: Send email every x minutes with image like in 2P
    subject = "Your lightsheet experiment is complete"

    yag = yagmail.SMTP(user=sender_email, password=sender_password)

    body = [
        "Hey!",
        "\n",
        "Your lightsheet experiment has finished and was a success! Come pick up your little fish",
        "\n" "fishgitbot",
    ]
    try:
        yag.send(
            to=receiver_email, subject=subject, contents=body,
        )
    except (
        yagmail.error.YagAddressError,
        yagmail.error.YagConnectionClosed,
        yagmail.error.YagInvalidEmailAddress,
    ):
        pass
