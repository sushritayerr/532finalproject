import pandas as pd


def clean_dataset(ds):
    # read dataset
    dataset = pd.read_csv(ds)
    # get list of distinct companies
    distinct_companies = dataset['company'].unique()
    # list of companies used in the project
    companies_for_analysis = ["Verizon", "Microsoft", "Google", "Nvidia", "Facebook"]

    # remove data of companies which are not in companies_for_analysis
    companies_to_be_removed = list(set(distinct_companies.tolist()) - set(companies_for_analysis))
    filtered_dataset = dataset[~dataset['company'].isin(companies_to_be_removed)]
    filtered_dataset = filtered_dataset[~filtered_dataset['sentiment'].isin(['Irrelevant'])]

    distinct_sentiment = filtered_dataset['sentiment'].unique()
    print(distinct_sentiment)

    # remove id column
    filtered_dataset = filtered_dataset.drop('id', axis=1)

    # move the column for sentiment to the end
    column_to_move = 'sentiment'
    other_columns = [col for col in filtered_dataset.columns if col != column_to_move]
    filtered_dataset = filtered_dataset[other_columns + [column_to_move]]

    print(filtered_dataset)

    # create csv file for filtered dataset
    filtered_dataset.to_csv('filtered_twitter_training.csv', index=False)


clean_dataset("twitter_training.csv")

