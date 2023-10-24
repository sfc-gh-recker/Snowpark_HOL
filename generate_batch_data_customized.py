from snowflake.snowpark.session import Session
import numpy as np
import random

SEASONS={
    "winter":{
        "lb":100000,
        "ub":2000000
    },
    "spring":{
        "lb":200000,
        "ub":4000000
    },
    "summer":{
        "lb":300000,
        "ub":4750000
    },
    "fall":{
        "lb":275000,
        "ub":4000000
    }
}

def generate_random_record(season_bounds=SEASONS) -> tuple:

    season = random.choice(list(season_bounds.keys()))

    return (
        season,
        np.random.randint(low=season_bounds[season]["lb"],high=season_bounds[season]["ub"]),
        np.random.randint(low=season_bounds[season]["lb"],high=season_bounds[season]["ub"]),
        np.random.randint(low=season_bounds[season]["lb"],high=season_bounds[season]["ub"]),
        np.random.randint(low=season_bounds[season]["lb"],high=season_bounds[season]["ub"]),
        np.random.randint(low=season_bounds[season]["lb"],high=season_bounds[season]["ub"]),
        np.random.randint(low=season_bounds[season]["lb"],high=season_bounds[season]["ub"]),
        np.random.randint(low=season_bounds[season]["lb"],high=season_bounds[season]["ub"]),
        np.random.randint(low=season_bounds[season]["lb"],high=season_bounds[season]["ub"]),
        np.random.randint(low=season_bounds[season]["lb"],high=season_bounds[season]["ub"]),
        np.random.randint(low=season_bounds[season]["lb"],high=season_bounds[season]["ub"])
    )


def generate_random_data(num_records: int) -> list:
    return [generate_random_record() for i in range(0, num_records)]


def generate_random_table(session: Session, num_records: int, table_name: str):

    print("generating random data...")
    new_df=session.create_dataframe(
        data=generate_random_data(num_records=num_records),
        schema=["SEASON","OFFLINE MARKETING","3RD PARTY LISTING","EMAIL","SOCIAL", "OTT","DIGITAL NOT SET","RADIO",  "SPEND FORM","DISPLAY","SEM"]
    )

    print("saving random data...")
    new_df.write.save_as_table(
        table_name=table_name,
        mode="overwrite"
    )

    print(f"{num_records} random records generated!")