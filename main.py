import profile_tweets
import mysql.connector
import urllib, json
import datetime
if __name__ == '__main__':
    
    cnx=mysql.connector.connect(user='user', password='',host='localhost',database='twitter')
    cursor = cnx.cursor(buffered=True)
    
    cursor.execute(("SET FOREIGN_KEY_CHECKS=0"))
    cursor.execute("Truncate table users")
    cursor.execute("Truncate table hashtags")
    cursor.execute("Truncate table mentions")
    cursor.execute("Truncate table tweets")
    cursor.execute("Truncate table conversation_id")
    cursor.execute("SET NAMES utf8mb4")
    cursor.execute("ALTER DATABASE twitter CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci")
    
    
    
    
    
    #profile_tweets.process_Mentions()
    #profile_tweets.process_Hashtags()
    #profile_tweets.process_Conversation()
    list=['elonmusk','cristiano']
    
    profile_tweets.fillUsernameDb(list)
    profile_tweets.update_Users_TableData()
    #for i in list:
    profile_tweets.run('elonmusk',10)
    #profile_tweets.process_Conversation()
    profile_tweets.process_Mentions()
    
    #profile_tweets.process_Hashtags()
    
    #profile_tweets.get_Tweets('cristiano')
    #profile_tweets.getUsernames()
    
    #"https://twitter.com/i/search/timeline?f=tweets&vertical=default&include_available_features=1&include_entities=1&reset_error_state=false&src=typd&max_position=-1&q=elonmusk"

