from prefect import flow
from prefect.blocks.notifications import MicrosoftTeamsWebhook


@flow(log_prints=True)
def hello_flow(name=""):
    print(f"Hello, {name}, this file has been changed again!")

    teams_webhook_block = MicrosoftTeamsWebhook.load("teams-test", validate=False)
    teams_webhook_block.notify(f"Flow executado para: {name}")

if __name__ == "__main__":
	hello_flow("world")