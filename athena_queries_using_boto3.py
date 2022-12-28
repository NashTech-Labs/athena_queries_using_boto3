CLIENT = boto3.client("athena")
RESULT_OUTPUT_LOCATION="s3://aki-athena-1/output/"

def has_query_succeeded(execution_id):
    state = "RUNNING"
    max_execution = 5

    while max_execution > 0 and state in ["RUNNING", "QUEUED"]:
        max_execution -= 1
        response = CLIENT.get_query_execution(QueryExecutionId=execution_id)
        if (
            "QueryExecution" in response
            and "Status" in response["QueryExecution"]
            and "State" in response["QueryExecution"]["Status"]
        ):
            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                return True

        time.sleep(30)

    return False



def get_num_rows():
    query = f"SELECT * FROM boto3.data limit 10"
    response = CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": RESULT_OUTPUT_LOCATION}
    )

    return response["QueryExecutionId"]



def get_query_results(execution_id):
    response = CLIENT.get_query_results(
        QueryExecutionId=execution_id
    )

    results = response['ResultSet']['Rows']
    return results

def main():
    # 5. Query Athena table
    execution_id = get_num_rows()
    print(f"Get Num Rows execution id: {execution_id}")

    query_status = has_query_succeeded(execution_id=execution_id)
    print(f"Query state: {query_status}")

    # 6. Query Results
    print(get_query_results(execution_id=execution_id))


if __name__ == "__main__":
    main()
