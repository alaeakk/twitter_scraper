import profile_tweets


if __name__ == '__main__':
    list=['cristiano','realMadrid']
    profile_tweets.fillUsernameDb(list)
    profile_tweets.run('cristiano',100)

