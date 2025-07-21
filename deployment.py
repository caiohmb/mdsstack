from prefect import flow, deploy

@flow
def flou():
    print("relouord")

if __name__ == "__main__":
    flou.deploy(name="localflou", work_pool_name="local-pool", image="prefecthq/prefect:3-latest")