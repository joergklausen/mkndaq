# -*- coding: utf-8 -*-
"""
Created on Sun Jun  2 08:40:51 2019

@author: joerg.klausen@alumni.ethz.ch
"""

# In[5]:
import os
import logging
import sqlite3
import time
import numpy as np
import pandas as pd
import yaml


class DatabaseHandler:
    """
    Manage data transfer to and from database.

    Manage data transfer to and from database.
    """

    @classmethod
    def __init__(self, config):
        """
        constructor

        Parameters
        ----------
        config : dict
            dictionary of attributes defining the database
        """
        try:                        

            # setup logging
            logdir = os.path.expanduser(config['logfile'])
            os.makedirs(logdir, exist_ok=True)
            logfile = '%s.log' % time.strftime('%Y%m%d')
            self.logfile = os.path.join(logdir, logfile)
            self.logger = logging.getLogger(__name__)
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                datefmt='%y-%m-%d %H:%M:%S',
                                filename=str(self.logfile),
                                filemode='a')
 
            # assign database
            self._dbfile = os.path.expanduser(config['dbfile'])
            self.logger.info("DatabaseHandler initialized for database '%s'." % (str(self._dbfile)))
            
        except Exception as err:
            self.logger.error('Error reading config_file.yaml', err)

    @classmethod
    def read_tbl(self, evg, from_date, to_date, period='M'):
        """
        Read rows from DB table
        
        Parameters
        ----------
        evg : str
            designation of EVG, [A-D]
        
        from_date : str
            Beginning of period, inclusive, of the form %Y-%m-%d
            
        to_date : str
            End of period, inclusive, of the form %Y-%m-%d
            
        Returns
        -------
            Pandas data frame
        
        """
        try:
            if period == 'M':
                tbl = "T_" + evg + "_MONTHLY"
            elif period == 'D':
                tbl = "T_" + evg + "_DAILY"
            qry = "SELECT * FROM %s WHERE dtm BETWEEN '%s 00:00:00' AND '%s 23:59:59'" % (tbl, from_date, to_date)
            
            # open db connection
            conn = sqlite3.connect(self._dbfile)
            
            # read DB table
            df = pd.read_sql_query(qry, conn)
            df['dtm'] = pd.to_datetime(df['dtm'],
                                      format="%Y-%m-%d %H:%M:%S")

            # stop if df is empty
            if len(df) == 0:
                raise Exception("Dataframe is empty.")

            return(df)

        except Exception as err:
            self.logger.error('Error reading table %s' % tbl, err)

    @classmethod
    def append_db(self, df, tbl, mode="bulk", key="dtm", verbose=True):
        """
        INSERT or UPDATE elements in tbl with elements from df
        
        Parameters
        ----------
        df : pd.dataframe
            Pandas dataframe object
            
        tbl : str
            table name
            
        mode : str
            Update mode. 
                'bulk': delete existing records, use pd.to_sql with if_exists='append'
                'by-element': --- currently not implemented ---
                    inserts new records (key=key), ignoring None values
                    updates existing records (key=key), ignoring None values
    
        key : str
            Database key
        
        Returns
        -------
        nothing
    
        """
        try:
            # open db connection
            conn = sqlite3.connect(self._dbfile)
    
            # check if DB table exists and count rows before appending new data
            try:
                qry = "SELECT COUNT(%s) AS cnt FROM %s" % (key, tbl)
                num_rows_start = int(pd.read_sql_query(qry, conn).cnt)
                num_rows_end = num_rows_start
            except Exception:
                num_rows_start = 0
    
            # stop if df is empty
            if len(df) == 0:
                num_rows_inserted = 0
                if verbose:
                    msg = "Dataframe is empty. Nothing to insert."
                    self.logger.info(msg)           
                
            else:
                cursor = conn.cursor()
                if mode == "bulk":
                    # remove existing records before inserting new ones
                    qry = "DELETE FROM %s WHERE %s BETWEEN '%s' AND '%s'" % (tbl, key, df[key].min(), df[key].max())
                    cursor.execute(qry)
                    conn.commit()
                    df.to_sql(tbl, conn, if_exists='append', index=False)
#                if mode == "by-row":        
#                    'by-row': iterates row-wise, only appends new records (key=key)
#                    for row in df.itertuples(index=False):
#                        print(row)
#                        # check if record for dtm already exists in tbl
#                        qry = "SELECT dtm FROM %s WHERE dtm='%s'" % (tbl, getattr(row, key))
#                        cursor.execute(qry)
#                        res = cursor.fetchone()
#                        if res != None:
#                            # record exists, need to delete first
#                            qry = "DELETE FROM %s WHERE dtm='%s'" % (tbl, getattr(row, key))
#                            cursor.execute(qry)
#                        # insert record
#                        df_ = df[df[key]==getattr(row, key)]
#                        df_.to_sql(tbl, conn, if_exists='append', index=False)
                if mode == "by-element":
                    print("under construction")
                    pass
        
                # verify number of records inserted
                qry = "SELECT COUNT(%s) as cnt FROM %s" % (key, tbl)
                num_rows_end = int(
                        pd.read_sql_query(qry, conn).cnt)
                num_rows_inserted = num_rows_end - num_rows_start                            
                if verbose:
                    msg = "%s: %i rows (appended: %i)" % (tbl, num_rows_end, num_rows_inserted)
                    self.logger.info(msg)
        
            conn.close()
    
            return (num_rows_end, num_rows_inserted)
    
        except Exception as err:
            conn.close()
            msg = "'.append_db' error: "
            self.logger.error(msg, err)


if __name__ == '__main__':
    pass
