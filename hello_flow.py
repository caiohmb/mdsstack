from prefect import flow

@flow(log_prints=True)
def hello_flow(name=""):
    print(f"Hello, {name}, this file has been changed!")

if __name__ == "__main__":
	hello_flow("world")