# TCdatalogger

Simple utility to that leverages the [Torn](https://www.torn.com/) City [API](https://api.torn.com/) to pull the data and drop it into Google BigQuery

To use this, the utility requires a:

- Torn City API key with faction API rights (`/config/TornCityAPIkey.txt`),
- Google BigQuery credentials (`/config/token.json`, `/config/credentials.json`), and
- write access to the Google Calendar. 



Create a service account that will have access to BigQuery. In the account, goto "Keys" > "Add Key" > "Create new key".  Select 'JSON' and save the file to './config/credentials.json'.

In TC, goto https://www.torn.com/preferences.php#tab=api and create a Full Access API key.  Save to './config/TornCityAPIkey.txt'.


