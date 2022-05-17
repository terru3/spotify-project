import pandas as pd
import numpy as np
import seaborn as sns

from tqdm import tqdm
from neo4j import GraphDatabase
import matplotlib.pyplot as plt

# Seen from :server status
uri = "bolt://localhost:7687"

auth = ("neo4j","jatdatares")

driver = GraphDatabase.driver(uri = uri, auth = auth)
print(driver.verify_connectivity())

class pagerank:
    def __init__(self, driver) -> None:
        """
        Args:
            driver is GraphDatabase.driver
        """
        self.driver = driver

    def close(self) -> None:
        self.driver.close()

    #estimation for memory
    @classmethod
    def write(cls, tx) -> any:
        """
        @param cls is the class
        @param tx is the transaction
        @returns the data for the memory
        """
        query = ("""
                    CALL gds.pageRank.write('myGraph', {
                    maxIterations: 20,
                    dampingFactor: 0.85,
                    writeProperty: 'pagerank'
                    })
                    YIELD nodePropertiesWritten, ranIterations
                    """)
        result = tx.run(query)
        # returns dataframe
        return result.data()

    @classmethod
    def pagerank(cls, tx) -> any:
        """
        @param cls is the class
        @param tx is the transaction
        @return result.data() is the data of the pagerank
        """
        query = ("""
                        Call gds.pageRank.stream('myGraph')
                        YIELD nodeId, score
                        RETURN gds.util.asNode(nodeId).name AS name, score AS pagerank,
                        gds.util.asNode(nodeId).playlist_id AS pid
                        ORDER BY score DESC, name ASC
                        LIMIT 50
                    """)
        result = tx.run(query)
        # return a dataframe
        return result.data()

    def write_pagerank(self) -> any:
        """
        @param self
        @return result is the result of the memory estimation
        """
        result = self.driver.session().write_transaction(self.write)
        return pd.DataFrame(result)

    def run_pagerank(self) -> any:
        """
        @param self
        @return result is the dataframe from the pagerank
        """
        result = self.driver.session().write_transaction(self.pagerank)
        return pd.DataFrame(result)

pagerank = pagerank(driver)
top50 = pagerank.run_pagerank()
print(top50.info())
print(top50.head())