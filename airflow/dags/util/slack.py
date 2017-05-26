from slackclient import SlackClient
from airflow.models import Variable

def send_message(channel, text=None, attachment=None):
    if text is None and attachment is None:
        raise Exception("Slack message must contain either text or attachment")

    api_params = {
        'channel'  : channel,
        'username' : 'Airflow',
        'icon_url' : 'https://airflow.incubator.apache.org/_images/pin_large.png'
    }

    if attachment is not None:
        api_params['attachments'] = [attachment]
    elif text is not None:
        api_params['text'] = text

    client = SlackClient(Variable.get('SlackToken'))
    resp   = client.api_call('chat.postMessage', **api_params)
    if not resp['ok']:
        raise Exception("Slack API call failed: ({})".format(resp['error']))
