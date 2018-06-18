# What's this

This is a project that collects json data from pre-defined data source, store them into a database, and store the raw json as files.
Currently, it only supports AWS S3 and RDS.

# How to start

## Setup AWS

- You can use a local database instead of RDS to run the project, but you will need to have at least a S3 bucket if you want to store the json files there.
- Run `aws configure` and setup the local develop environment.

## Setup the PC

pipenv and python is necessary to run this project.

Make a new file called `.env` at the root of the project. The following keys are necessary for now.

- `BUCKET_NAME`: The bucket name
- `DATABASE_URL`: Where is the database
- `NYT_API_KEYS`: Valid NYT API keys separated by `_`. For example: onenytkeyabc_anothernytkeyabc

## Setup the project

```bash
# The paper work: git clone and cd into the project

pipenv install --dev # Install all the dependencies

pipenv shell # Spawns a shell within the virtualenv

pipenv run python daily_collector.py
```

# The interface for adding new datasource

- Refer to the following code snippet for simple explanation.
- Refer to `NYT.py` to see more details.

```py
class XXX(object):
    def __init__(self, target_date, s3_bucket):
        self.label_name = 'XXX'
        self.target_date = target_date
        self.s3_bucket = s3_bucket

        self.logger = logging.getLogger(
            'daily_collector.{}'.format(self.__class__.__name__))

        self.events = # Fill in this list with objects that collected from the data source.

    def already_same(self, existing_event, row):
        # This method tells the database should the row in database be considered the same or not
        # It returns a boolean indicating that whether `existing_event` is the same as `row`
        # existing_event is a ResultProxy object, while row is a Python dictionary

    def map_json_array_to_rows(self, json_array, label_id):
        # This methods maps json_array (aka: self.events) to a list of rows that Database knows how to handle.
        result = []
        for jsevt in json_array:
            try:
                result.append({
                    'timestamp': # pick/calculated the right field,
                    'title': # pick/calculated the right field,
                    'text': # pick/calculated the right field,
                    'link': # pick/calculated the right field,
                    'label_id': label_id,
                    'image_link': # pick/calculated the right field,
                    'media_link': # pick/calculated the right field
                })
            except Exception as exception:
                self.logger.error('{}') # Some information that saying which part went wrong.
        return result

    def store_s3(self):
        # Pick the right name for json files.
        if len(self.events) > 0:
            self.s3_bucket.Object(key='{}/{}.json'.format(self.label_name,
                                                          self.format_date(self.target_date, with_hyphen=True))).put(Body=json.dumps(self.events, indent=2))
            self.logger.info('Successfully stored {} {} events into S3'.format(
                self.format_date(self.target_date), len(self.events)))
        else:
            self.logger.warn(
                '***** No data for {} so skipping... '.format(self.format_date(self.target_date)))

    def store_rds(self, db):
        # Get the label_id from the database
        label_id = db.get_label_id_from_name(self.label_name)

        # Prepare the rows that will be processed by Database
        rows = self.map_json_array_to_rows(self.events, label_id)

        # Delegate Database to handle the Persistance logic.
        # In the method call to db.store_rds, each row is inspected towards the database.
        # If there already something, check whether it is up to date based on the third argument(self.already_same).
        #   If what's in database is already up to date, ignore
        #   Otherwise update the database to the newest state
        # Otherwise insert the row into the database
        # And the function returns number of each actions it carried out.
        no_inserts, no_updates, no_notouch = db.store_rds(
            rows, label_id, self.already_same)
        self.logger.info('{} Total from json:{:>5} Inserted: {:>5} Updated: {:>5} Up-to-date: {:>5}'.format(
            self.target_date,
            len(rows),
            no_inserts,
            no_updates,
            no_notouch
        ))
```

# How to run

- To run the function locally, do `pipenv run python daily_collector.py` and see the standard output for the results.
  - `pipenv run` will read in `.env` file into the process.
- To run the function on AWS Lambda, make a deployment package and upload it to Lambda as documented [here](https://docs.aws.amazon.com/lambda/latest/dg/lambda-python-how-to-create-deployment-package.html).
