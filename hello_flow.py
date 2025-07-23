from prefect import flow
from prefect.blocks.system import Secret


@flow(log_prints=True)
def hello_flow(name=""):
    secret_block = Secret.load("block-test")
    print(f"Hello, {name}, this is block test {secret_block}!")

if __name__ == "__main__":
	hello_flow("world")