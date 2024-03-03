from cassandra.cluster import Cluster


class CassandraConnector:
    """
    A class for connecting to and interacting with a Cassandra database.
    """

    def __init__(self, keyspace: str, logging):
        """
        Initializes a CassandraConnector instance.

        Parameters:
        - keyspace (str): The keyspace to connect to.
        """
        self.keyspace = keyspace
        self.cluster = Cluster(["127.0.0.1"], port=9042)
        self.session = self.cluster.connect(keyspace)
        self.ensure_tables(logging)
        self.prepare_query(logging)

    def prepare_query(self, logging):
        """
        Prepares the insert query.
        """
        insert_query = "INSERT INTO profiles JSON ?"
        try:
            self.prepared_stmt = self.session.prepare(insert_query)
            logging.info("Query prepared.")
        except Exception as e:
            logging.warning("Error preparing query: {e}")

    def insert_to_cassandra(self, profile_json, logging):
        """
        Inserts a JSON profile into the Cassandra database.

        Parameters:
        - profile_json (str): The JSON profile to insert.
        """
        while True:
            try:
                self.session.execute(self.prepared_stmt, [profile_json])
                logging.info("data inserted successfully")
                break
            except Exception as e:
                logging.warning(f"Error inserting into Cassandra: {e}")
                continue

    def ensure_tables(self, logging) -> None:
        """Ensures that the necessary tables exist in the keyspace."""
        create_table_statements = [
            """
            CREATE TYPE IF NOT EXISTS skill (
                skill TEXT,
                weight INT
                );
                """,
            """
            CREATE TYPE IF NOT EXISTS oss_contribution (
            id INT,
            project_name TEXT,
            github_url TEXT,
            first_commit_date TIMESTAMP,
            last_commit_date TIMESTAMP,
            num_of_commits INT,
            summary TEXT,
            topics LIST<TEXT>,
            prog_rank INT
            );
                """,
            """
            CREATE TYPE IF NOT EXISTS work_experience (
            company TEXT,
            companyLinkedInUrl TEXT,
            companyUrl TEXT,
            companyUuid TEXT,
            country TEXT,
            countryId INT,
            current INT,
            industry TEXT,
            location TEXT,
            locationId INT,
            maxEmployeeSize TEXT,
            minEmployeeSize TEXT,
            sequenceNo INT,
            startDate TIMESTAMP,
            endDate TIMESTAMP,
            title TEXT,
            summary TEXT
            );
                """,
            """
            CREATE TYPE IF NOT EXISTS education (
            campus TEXT,
            campusUuid TEXT,
            current INT,
            endDate TIMESTAMP,
            major TEXT,
            sequenceNo INT,
            specialization TEXT,
            startDate TIMESTAMP
            );
                """,
            """
            CREATE TYPE IF NOT EXISTS certification ( 
            beginDate TEXT,
            description TEXT,
            endDate TEXT,
            reference TEXT,
            title TEXT
            );
                """,
            """
            CREATE TYPE IF NOT EXISTS  memberships(
            beginDate TEXT,
            endDate TEXT,
            name  TEXT,
            title TEXT
            );
                """,
            """
            CREATE TYPE  IF NOT EXISTS publications(
            date TIMESTAMP,
            description TEXT,
            issue  TEXT,
            title TEXT,
            url TEXT
            );
                """,
            """
            CREATE TYPE IF NOT EXISTS patents(
            date TEXT,
            description TEXT,
            reference TEXT,
            title TEXT,
            url TEXT
            );
                """,
            """
            CREATE TABLE IF NOT EXISTS profiles (
                id INT PRIMARY KEY,
                profile_pic_url TEXT,
                full_name TEXT,
                first_name TEXT,
                last_name TEXT,
                github_handle TEXT,
                linkedin_url TEXT,
                github_url TEXT,
                twitter_url TEXT,
                seniority_level TEXT,
                emails LIST<TEXT>,
                primary_email TEXT,
                title TEXT,
                location TEXT,
                summary TEXT,
                country_id INT,
                country TEXT,
                bio TEXT,
                company TEXT,
                skills LIST<FROZEN<skill>>,
                programming_languages LIST<TEXT>,
                years_of_employment INT,
                years_of_experience INT,
                oss_contributions LIST<FROZEN<oss_contribution>>,
                work_experiences LIST<FROZEN<work_experience>>,
                educations LIST<FROZEN<education>>,
                publications LIST<FROZEN<publications>>, 
                certifications LIST<FROZEN<certification>>,
                patents LIST<FROZEN<patents>>,
                memberships LIST<FROZEN<memberships>>,
                awards LIST<TEXT>,
                languages LIST<TEXT>,
                expertise LIST<TEXT>,
                top_schools LIST<TEXT>,
                likely_to_move_prob DOUBLE,
                is_first_name_female BOOLEAN,
                weight  INT,
                match_score TEXT,
                region TEXT,
                sub_region TEXT, 
            );
            """,
        ]

        for statement in create_table_statements:

            try:
                self.session.execute(statement)
                logging.info("Table created or verified:")
            except Exception as e:
                logging.warning(f"Error inserting into Cassandra: {e}")
                raise
