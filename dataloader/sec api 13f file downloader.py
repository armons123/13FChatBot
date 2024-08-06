import os
import pymysql
from sec_api import QueryApi
import pandas as pd

# git change
class Filing13F:
    def __init__(self, periodOfReport: str, queryApi: QueryApi, db_connection):
        self.queryApi = queryApi
        self.periodOfReport = periodOfReport
        self.filings = []
        self.db_connection = db_connection

    def download_13f_filings(self, start: int, size: int):
        queryStr = f'formType:"13F-HR" AND NOT formType:"13F-HR/A" AND periodOfReport:"{self.periodOfReport}"'
        query = {
            "query": queryStr,
            "from": start,
            "size": size,
            "sort": [{"filedAt": {"order": "desc"}}]
        }

        response = self.queryApi.get_filings(query)
        self.filings.extend(response['filings'])

    def get_total_filings(self):
        queryStr = f'formType:"13F-HR" AND NOT formType:"13F-HR/A" AND periodOfReport:"{self.periodOfReport}"'
        query = {
            "query": queryStr,
            "from": 0,
            "size": 1,
            "sort": [{"filedAt": {"order": "desc"}}]
        }

        response = self.queryApi.get_filings(query)
        return response['total']['value']

    def save_filing_to_db(self):
        filingsDataframe = pd.json_normalize(self.filings, max_level=0)
        filingsDataframe = filingsDataframe.drop(
            columns=['holdings', 'entities', 'dataFiles', 'documentFormatFiles', 'seriesAndClassesContractsInformation', 'linkToXbrl'],
            axis=1
        )

        if 'linkToTxt' in filingsDataframe.columns:
            filingsDataframe = filingsDataframe.rename(columns={'linkToTxt': 'linkToText'})

        if 'filedAt' in filingsDataframe.columns:
            filingsDataframe[['filedAt_datetime', 'filedAt_timezone']] = filingsDataframe['filedAt'].str.extract(r'(.*)([+-]\d{2}:\d{2})')
            filingsDataframe = filingsDataframe.drop(columns=['filedAt'])

        def convert_date_format(date_str):
            try:
                return pd.to_datetime(date_str).strftime("%Y-%m-%d")
            except ValueError:
                return date_str

        if 'periodOfReport' in filingsDataframe.columns:
            filingsDataframe['periodOfReport'] = filingsDataframe['periodOfReport'].apply(convert_date_format)
        if 'effectivenessDate' in filingsDataframe.columns:
            filingsDataframe['effectivenessDate'] = filingsDataframe['effectivenessDate'].apply(convert_date_format)

        # Replace NaN values with None
        filingsDataframe = filingsDataframe.where(pd.notnull(filingsDataframe), None)

        with self.db_connection.cursor() as cursor:
            for _, row in filingsDataframe.iterrows():
                sql = """
                    INSERT INTO filings (
                        id, accessionNo, cik, ticker, companyName, companyNameLong, formType, description, linkToText, 
                        linkToHtml, linkToFilingDetails, periodOfReport, effectivenessDate, filedAt_datetime, filedAt_timezone
                    ) VALUES (
                        %(id)s, %(accessionNo)s, %(cik)s, %(ticker)s, %(companyName)s, %(companyNameLong)s, %(formType)s, 
                        %(description)s, %(linkToText)s, %(linkToHtml)s, %(linkToFilingDetails)s, %(periodOfReport)s, 
                        %(effectivenessDate)s, %(filedAt_datetime)s, %(filedAt_timezone)s
                    )
                    ON DUPLICATE KEY UPDATE
                        accessionNo=VALUES(accessionNo),
                        cik=VALUES(cik),
                        ticker=VALUES(ticker),
                        companyName=VALUES(companyName),
                        companyNameLong=VALUES(companyNameLong),
                        formType=VALUES(formType),
                        description=VALUES(description),
                        linkToText=VALUES(linkToText),
                        linkToHtml=VALUES(linkToHtml),
                        linkToFilingDetails=VALUES(linkToFilingDetails),
                        periodOfReport=VALUES(periodOfReport),
                        effectivenessDate=VALUES(effectivenessDate),
                        filedAt_datetime=VALUES(filedAt_datetime),
                        filedAt_timezone=VALUES(filedAt_timezone)
                """
                cursor.execute(sql, row.to_dict())
            self.db_connection.commit()

    def save_holdings_to_db(self):
        holdingsList = []
        for filing in self.filings:
            if 'holdings' in filing:
                holdingsList.extend(pd.json_normalize(filing['holdings']).assign(filing_id=filing["id"]) for _ in [filing])

        if holdingsList:
            holdingsDataframe = pd.concat(holdingsList, ignore_index=True)

            # Replace NaN values with None
            holdingsDataframe = holdingsDataframe.where(pd.notnull(holdingsDataframe), None)

            # Rename columns to match database schema
            column_rename_map = {
                'votingAuthority.Sole': 'votingAuthority_Sole',
                'votingAuthority.Shared': 'votingAuthority_Shared',
                'votingAuthority.None': 'votingAuthority_None'
            }
            holdingsDataframe = holdingsDataframe.rename(columns=column_rename_map)

            # Ensure all expected columns are present
            expected_columns = [
                'cusip', 'ticker', 'cik', 'investmentDiscretion', 'nameOfIssuer', 'value', 'titleOfClass', 
                'votingAuthority_Sole', 'votingAuthority_Shared', 'votingAuthority_None', 
                'shrsOrPrnAmt_Type', 'shrsOrPrnAmt', 'filing_id', 'otherManager', 'putCall'
            ]
            for column in expected_columns:
                if column not in holdingsDataframe.columns:
                    holdingsDataframe[column] = None

            # Convert 'cik' to integer and handle invalid values
            holdingsDataframe['cik'] = holdingsDataframe['cik'].apply(lambda x: int(x) if pd.notnull(x) and x != '' else None)

            # Convert 'value' to numeric and handle invalid values
            holdingsDataframe['value'] = pd.to_numeric(holdingsDataframe['value'], errors='coerce')

            # Ensure all columns are explicitly converted to strings to avoid NaN issues
            holdingsDataframe = holdingsDataframe.astype(object).where(pd.notnull(holdingsDataframe), None)

            # Check for out-of-range values in 'value' and print them
            max_decimal_value = 10**18 - 0.01  # Adjust this value based on your DECIMAL precision and scale
            out_of_range_values = holdingsDataframe[holdingsDataframe['value'] > max_decimal_value]

            if not out_of_range_values.empty:
                print("Out of range values found in 'value' column:")
                print(out_of_range_values[['cusip', 'ticker', 'value']])

            # Clip out-of-range values to the maximum allowed value
            holdingsDataframe['value'] = holdingsDataframe['value'].apply(lambda x: x if x is None or x <= max_decimal_value else max_decimal_value)

            with self.db_connection.cursor() as cursor:
                for _, row in holdingsDataframe.iterrows():
                    try:
                        sql = """
                            INSERT INTO holdings (
                                cusip, ticker, cik, investmentDiscretion, nameOfIssuer, value, titleOfClass, 
                                votingAuthority_Sole, votingAuthority_Shared, votingAuthority_None, 
                                shrsOrPrnAmt_Type, shrsOrPrnAmt, filing_id, otherManager, putCall
                            ) VALUES (
                                %(cusip)s, %(ticker)s, %(cik)s, %(investmentDiscretion)s, %(nameOfIssuer)s, %(value)s, 
                                %(titleOfClass)s, %(votingAuthority_Sole)s, %(votingAuthority_Shared)s, %(votingAuthority_None)s, 
                                %(shrsOrPrnAmt_Type)s, %(shrsOrPrnAmt)s, %(filing_id)s, %(otherManager)s, %(putCall)s
                            )
                        """
                        cursor.execute(sql, row.to_dict())
                    except pymysql.err.DataError as e:
                        print(f"DataError: {e} for row: {row.to_dict()}")
                        raise
                self.db_connection.commit()
        else:
            print(f'No holdings data to save')


if __name__ == "__main__":
    # Initialize the query API
    queryApi = QueryApi(api_key="1da6ec6e57021b3ba0623bbdcd6e2a46c78083847cff24bbe7e54f32bd8569cc")

    # Database connection
    db_connection = pymysql.connect(
        host='localhost',
        user='root',
        password='Falcons2387!',
        db='13f',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        # Create a new 13F filing object
        period_of_report = "2024-03-31"
        filing13F = Filing13F(periodOfReport=period_of_report, queryApi=queryApi, db_connection=db_connection)

        # Download the 13F filings
        max_value = filing13F.get_total_filings()
        size = 200

        for start in range(0, max_value, size):
            if start + size > max_value:
                # Handle the last, potentially incomplete iteration
                filing13F.download_13f_filings(start, max_value - start)
            else:
                filing13F.download_13f_filings(start, size)

            # Save each batch of filings to the database
            filing13F.save_filing_to_db()
            filing13F.save_holdings_to_db()

            # Reset filings list for the next batch
            filing13F.filings = []

    finally:
        db_connection.close()



    