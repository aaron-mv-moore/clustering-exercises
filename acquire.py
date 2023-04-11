import pandas as pd
import os
import env

def get_zillow():
    '''
    Argument: No arguments required
    Actions: 
        1. Checks for the existence of the csv in the current directory
            a. if present:
                i. reads the csv
            b. if not present:
                i. queries MySQL dtabase using the env.py file for the credentials
                ii. saves the csv to the current working directory
    Return: dataframe
    Modules: 
        import env
        import pandas as pd
        import os
    '''
    # a variable to hold the xpected or future file name
    filename = 'zillow.csv'

    # if the file is present in the directory 
    if os.path.isfile(filename):

        # read the csv and assign it to the variable df
        df = pd.read_csv(filename, index_col=0)

        # return the dataframe and exit the funtion
        return df

    # if the file is not in the current working directory,
    else:
        # assign the name of the database to db
        db = 'zillow'

        # use the env.py function to get the url needed from the db
        url = env.get_db_url(db)

        # assign the sql query into the variable query
        query = '''SELECT
                      *
                    FROM properties_2017 p7
                      LEFT OUTER JOIN predictions_2017 ON p7.parcelid = predictions_2017.parcelid
                      LEFT OUTER JOIN airconditioningtype act ON p7.airconditioningtypeid = act.airconditioningtypeid
                      LEFT OUTER JOIN architecturalstyletype ast ON p7.architecturalstyletypeid = ast.architecturalstyletypeid
                      LEFT OUTER JOIN buildingclasstype bct ON p7.buildingclasstypeid = bct.buildingclasstypeid
                      LEFT OUTER JOIN heatingorsystemtype hst ON p7.heatingorsystemtypeid = hst.heatingorsystemtypeid
                      LEFT OUTER JOIN propertylandusetype plut ON p7.propertylandusetypeid = plut.propertylandusetypeid
                      LEFT OUTER JOIN storytype st ON p7.storytypeid = st.storytypeid
                      LEFT OUTER JOIN typeconstructiontype tct ON p7.typeconstructiontypeid = tct.typeconstructiontypeid
                    WHERE YEAR(predictions_2017.transactiondate) = 2017 
                      AND (latitude IS NOT NULL AND longitude IS NOT NULL);'''

        # query sql using pandas function
        df = pd.read_sql(query, url)

        # save the dataframe as a csv to the current working directory
        df.to_csv(filename)

        # returns the dataframe
        return df
