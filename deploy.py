from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/caiohmb/mdsstack.git",
        entrypoint="hello_flow.py:hello_flow",
    ).deploy(
        name="my-first-deployment",
        parameters={
            'name': 'CDB - Data Solutions'
        },
        work_pool_name="default",
        cron="* * * * *",  # Run every munite
    )