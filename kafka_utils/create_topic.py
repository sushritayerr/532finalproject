from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def create_company_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id='topic_creator'
    )

    topic_list = []
    companies = ["Verizon", "Microsoft", "Google", "Nvidia", "Facebook"]
    for company in companies:
        topic_list.append(NewTopic(name=company, num_partitions=1, replication_factor=1))

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except TopicAlreadyExistsError as e:
        return
