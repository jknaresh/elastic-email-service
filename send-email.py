import configparser
import time
import ElasticEmail
from ElasticEmail.apis.tags import emails_api
from ElasticEmail.model.email_content import EmailContent
from ElasticEmail.model.body_part import BodyPart
from ElasticEmail.model.body_content_type import BodyContentType
from ElasticEmail.model.email_recipient import EmailRecipient
from ElasticEmail.model.email_message_data import EmailMessageData
import pandas as pd
from uuid import uuid4
from pprint import pprint
from pathlib import Path
import re

from utils import get_batch, batch_sizes_dict

# Regular expression for validating an Email
email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'


def to_capitalize_case(text):
    # Split the text into words based on space or any
    # non-alphanumeric character
    words = re.split(r'\W+', text)

    # Capitalize the first letter of each word and join them together
    capitalize_case_text = ' '.join(word.capitalize() for word in words if
                                    word)

    return capitalize_case_text


def is_valid_email(email1):
    if email1 and re.match(email_regex, email1):
        return True
    else:
        return False


def send_email(
        name1, to_email, subject, body_html, body_text, from_email, reply_to):
    email_message_data = EmailMessageData(
        Recipients=[
            EmailRecipient(
                Email=to_email,
                Fields={
                    "name": name1,
                },
            ),
        ],
        Content=EmailContent(
            body=[
                BodyPart(
                    ContentType=BodyContentType("HTML"),
                    Content=body_html,
                    Charset="utf-8",
                ),
                BodyPart(
                    ContentType=BodyContentType("PlainText"),
                    Content=body_text,
                    Charset="utf-8",
                ),
            ],
            From=from_email,
            ReplyTo=reply_to,
            Subject=subject,
        ),
    )
    # EmailMessageData | Email data

    try:
        # Send Bulk Emails
        api_response = api_instance.emails_post(email_message_data)
        pprint(api_response)
        return api_response
    except ElasticEmail.ApiException as e:
        print("Exception when calling EmailsApi->emails_post: %s\n" % e)
    return None


BASE_DIR = Path(__file__).resolve().parent


# look for prod config file.
CONFIG_FILE = Path(BASE_DIR, "_prod.ini")
if not CONFIG_FILE.exists():
    # look for dev config file.
    CONFIG_FILE = Path(BASE_DIR, "_dev.ini")

CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_FILE)

# Defining the host is optional and defaults to https://api.elasticemail.com/v4
configuration = ElasticEmail.Configuration()

# Configure API key authorization: apikey
configuration.api_key['apikey'] = CONFIG.get("DEFAULT", "APIKEY")

campaign_folder = "camp2"

campaign_base = BASE_DIR / "campaigns"
emails_file = campaign_base / campaign_folder / "emails.csv"
batch_number_file = campaign_base / campaign_folder / "batch_number.txt"
subject_file = campaign_base / campaign_folder / "subject.txt"
html_file = campaign_base / campaign_folder / "html.html"
text_file = campaign_base / campaign_folder / "text.txt"
report_file = str(time.time()) + "_report.csv"
report_file = (campaign_base / campaign_folder / report_file)

df = pd.read_csv(emails_file)
df = df.dropna()
df.drop_duplicates(inplace=True)

batch_number = 1
if batch_number_file.exists():
    batch_number = int(open(batch_number_file).read())

try:
    batch_df = get_batch(df, batch_number, batch_sizes=batch_sizes_dict)
    batch_df = batch_df.copy()
    batch_df["capitalize_name"] = batch_df["name"].apply(to_capitalize_case)
    print(f"Batch {batch_number} contains {batch_df.shape} rows")
    print(f"Batch min {batch_df.index.min()} max {batch_df.index.max()} "
          f"index.")

    with ElasticEmail.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = emails_api.EmailsApi(api_client)
        for index, row in batch_df.iterrows():
            name = row["capitalize_name"]
            email = row["email"]
            if not is_valid_email(email):
                batch_df.at[index, 'is_valid_email'] = False
                continue
            batch_df.at[index, 'is_valid_email'] = True
            f_email = "CA Muppala Sreedhar<info@camuppalasreedhar.in>"
            res = send_email(
                name1=name.strip(),
                to_email=email.strip(),
                subject=open(subject_file).read(),
                body_html=open(html_file).read(),
                body_text=open(text_file).read(),
                from_email=f_email,
                reply_to="info@camuppalasreedhar.in"
            )
            if not res:
                continue

            try:
                r = res.response.json()
                batch_df.at[index, 'TransactionID'] = r.get('TransactionID')
                batch_df.at[index, 'MessageID'] = r.get('MessageID')
            except Exception as e:
                print(e)

        batch_df.to_csv(report_file)
        with open(batch_number_file, "w") as f:
            f.write(str(batch_number+1))

except (ValueError, IndexError) as e:
    print(e)
