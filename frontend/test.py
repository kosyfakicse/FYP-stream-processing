import requests

"""
run.bat serve -> start the backend application

"""
TIMEOUT = 30
RUN_TIMEOUT = 120
BASE_URL = "http://localhost:8000"

sample_dataset = [
    ("user_1", "#DATA is awesome" ,1793844033600),
    ("user_3", "#AI is awesome" ,1793844033600),
    ("user_2", "#PYTHON is awesome" ,1793844033600),
    ("user_1", "#AI is awesome" ,1793844033600),
    ("user_2", "#AI is awesome" ,1793844033600)
]

dataset_name = "tweets1"

def to_csv(dataset):
    lines = ["user_id,text,timestamp"]
    lines += [f"{user_id},{text},{timestamp}" for user_id, text, timestamp in dataset]
    return "\n".join(lines) + "\n"


def post_dataset():
    return requests.post(
        f"{BASE_URL}/datasets",
        params={
            "name": dataset_name
        },
        data=to_csv(sample_dataset),
        headers={"Content-Type": "text/csv"},
        timeout=TIMEOUT,
    )


def get_dataset():
    return requests.get(f"{BASE_URL}/datasets", timeout=TIMEOUT)

def post_run(dataset_id):
    return requests.post(f"{BASE_URL}/runs", json={"datasetId": dataset_id}, timeout=RUN_TIMEOUT)


def get_runs():
    return requests.get(f"{BASE_URL}/runs", timeout=TIMEOUT)


def get_run(run_id):
    return requests.get(f"{BASE_URL}/runs/{run_id}", timeout=TIMEOUT)


def get_run_graph(run_id):
    return requests.get(f"{BASE_URL}/runs/{run_id}/graph", timeout=TIMEOUT)



def main():
    try:
        response = post_dataset()
        print(response.status_code)
        dataset_id = response.json()["datasetId"]
        print(dataset_id)

        response = post_run(dataset_id)
        print(response.status_code)
        run_id = response.json()["runId"]
        print(run_id)

        response = get_run(run_id)
        print(response.status_code)
        print(response.json())

        response = get_run_graph(run_id)
        print(response.status_code)
        print(response.json())

    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
