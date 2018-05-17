package twitter;

import java.net.UnknownHostException;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

import twitter4j.DirectMessage;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.UserStreamListener;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import twitter4j.UserList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import java.util.Map;
import java.util.TreeMap;

public class Main {

	private static MongoCollection<Document> collection_a, collection_b;
	private static Map<String, LinkedList<String>> index = new TreeMap<>();
	private static Map<String, String> tweets = new HashMap<>();

	public static void main(String args[]) throws InterruptedException,
			UnknownHostException, TwitterException {

		// Mongoclient
		MongoClient mongoClient = null;
		MongoDatabase database = null;

		// set up Maps and index
		init();

		// connect to mongoDB
		// First mongoDB is for storing tweets straight into the document in
		// col_a
		connectmongoDB_a(mongoClient, database, "twitterDB", "col_a");
		// Second mongoDB is for storing indexed data into the document in col_b
		connectmongoDB_b(mongoClient, database, "twitterDB", "col_b");

		// Instance creation to use Twitter API - OAuth Authentication (Always
		// Required to use TwitterAPI)
		// Use configuration builder in Twitter4J to set the login information
		ConfigurationBuilder cb = new ConfigurationBuilder();
		// sets the login information that was created using Twitter Dev
		// Application (ConsumerKey, ConsumerSecret, AccessToken,
		// AccessTokenSecret)
		cb.setDebugEnabled(true)
				.setOAuthConsumerKey("coxwDHqwv25OXCfSLdQCLUGjd")
				.setOAuthConsumerSecret(
						"Uq2XvdzFxzgOEABeNV9Ua91UjltaBgE0qkykukIKZV0PQMLmIh")
				.setOAuthAccessToken(
						"988621776799252481-WUOX2WzePyfGiCfOT2NROJKviZNjE9D")
				.setOAuthAccessTokenSecret(
						"70a8LinTfoAOqJZIEnAXrI89z5SUXaRufac3TBWopvZjF");

		// Initiate twitter stream using authentication
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build())
				.getInstance();

		// Add listener
		UserStreamListener listener = new UserStreamListener() {
			// onStatus method is the method for getting the real-time tweets
			public void onStatus(Status status) {
				// System.out.println("onStatus @" +
				// status.getUser().getScreenName() + " - " +
				// status.getText()+"\n\n");
				// Everytime new status comes up, new document is created

				// Create new document instance
				Document document_a = new Document();
				// append the data into the document
				document_a.append("tweet_user", status.getUser()
						.getScreenName());
				document_a.append("text", status.getText());
				collection_a.insertOne(document_a);
				System.out.println(document_a);

				// put tweets into the Map
				puttweets(status.getText());
				// build inverted index
				invertindex();
				// create document made up of the index data
				for (Map.Entry<String, LinkedList<String>> entry : index
						.entrySet()) {
					createDocument(entry.getKey(), entry.getValue());
				}
			}

			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:"
						+ statusDeletionNotice.getStatusId());
			}

			public void onDeletionNotice(long directMessageId, long userId) {
				System.out.println("Got a direct message deletion notice id:"
						+ directMessageId);
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got a track limitation notice:"
						+ numberOfLimitedStatuses);
			}

			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId
						+ " upToStatusId:" + upToStatusId);
			}

			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
			}

			public void onFriendList(long[] friendIds) {
				System.out.print("onFriendList");
				for (long friendId : friendIds) {
					System.out.print(" " + friendId);
				}
				System.out.println("\n");
			}

			public void onFavorite(User source, User target,
					Status favoritedStatus) {
				System.out.println("onFavorite source:@"
						+ source.getScreenName() + " target:@"
						+ target.getScreenName() + " @"
						+ favoritedStatus.getUser().getScreenName() + " - "
						+ favoritedStatus.getText());
			}

			public void onUnfavorite(User source, User target,
					Status unfavoritedStatus) {
				System.out.println("onUnFavorite source:@"
						+ source.getScreenName() + " target:@"
						+ target.getScreenName() + " @"
						+ unfavoritedStatus.getUser().getScreenName() + " - "
						+ unfavoritedStatus.getText());
			}

			public void onFollow(User source, User followedUser) {
				System.out.println("onFollow source:@" + source.getScreenName()
						+ " target:@" + followedUser.getScreenName() + "\n");
			}

			public void onUnfollow(User source, User followedUser) {
				System.out.println("onFollow source:@" + source.getScreenName()
						+ " target:@" + followedUser.getScreenName());
			}

			public void onDirectMessage(DirectMessage directMessage) {
				System.out.println("onDirectMessage text:"
						+ directMessage.getText());
			}

			public void onUserListMemberAddition(User addedMember,
					User listOwner, UserList list) {
				System.out
						.println("onUserListMemberAddition added member:@"
								+ addedMember.getScreenName() + " listOwner:@"
								+ listOwner.getScreenName() + " list:"
								+ list.getName());
			}

			public void onUserListMemberDeletion(User deletedMember,
					User listOwner, UserList list) {
				System.out
						.println("onUserListMemberDeleted deleted member:@"
								+ deletedMember.getScreenName()
								+ " listOwner:@" + listOwner.getScreenName()
								+ " list:" + list.getName());
			}

			public void onUserListSubscription(User subscriber, User listOwner,
					UserList list) {
				System.out
						.println("onUserListSubscribed subscriber:@"
								+ subscriber.getScreenName() + " listOwner:@"
								+ listOwner.getScreenName() + " list:"
								+ list.getName());
			}

			public void onUserListUnsubscription(User subscriber,
					User listOwner, UserList list) {
				System.out
						.println("onUserListUnsubscribed subscriber:@"
								+ subscriber.getScreenName() + " listOwner:@"
								+ listOwner.getScreenName() + " list:"
								+ list.getName());
			}

			public void onUserListCreation(User listOwner, UserList list) {
				System.out
						.println("onUserListCreated  listOwner:@"
								+ listOwner.getScreenName() + " list:"
								+ list.getName());
			}

			public void onUserListUpdate(User listOwner, UserList list) {
				System.out
						.println("onUserListUpdated  listOwner:@"
								+ listOwner.getScreenName() + " list:"
								+ list.getName());
			}

			public void onUserListDeletion(User listOwner, UserList list) {
				System.out
						.println("onUserListDestroyed  listOwner:@"
								+ listOwner.getScreenName() + " list:"
								+ list.getName());
			}

			public void onUserProfileUpdate(User updatedUser) {
				System.out.println("onUserProfileUpdated user:@"
						+ updatedUser.getScreenName());
			}

			public void onUserDeletion(long deletedUser) {
				System.out.println("onUserDeletion user:@" + deletedUser);
			}

			public void onUserSuspension(long suspendedUser) {
				System.out.println("onUserSuspension user:@" + suspendedUser);
			}

			public void onBlock(User source, User blockedUser) {
				System.out.println("onBlock source:@" + source.getScreenName()
						+ " target:@" + blockedUser.getScreenName());
			}

			public void onUnblock(User source, User unblockedUser) {
				System.out.println("onUnblock source:@"
						+ source.getScreenName() + " target:@"
						+ unblockedUser.getScreenName());
			}

			public void onRetweetedRetweet(User source, User target,
					Status retweetedStatus) {
				System.out.println("onRetweetedRetweet source:@"
						+ source.getScreenName() + " target:@"
						+ target.getScreenName()
						+ retweetedStatus.getUser().getScreenName() + " - "
						+ retweetedStatus.getText());
			}

			public void onFavoritedRetweet(User source, User target,
					Status favoritedRetweet) {
				System.out.println("onFavroitedRetweet source:@"
						+ source.getScreenName() + " target:@"
						+ target.getScreenName()
						+ favoritedRetweet.getUser().getScreenName() + " - "
						+ favoritedRetweet.getText());
			}

			public void onQuotedTweet(User source, User target,
					Status quotingTweet) {
				System.out.println("onQuotedTweet" + source.getScreenName()
						+ " target:@" + target.getScreenName()
						+ quotingTweet.getUser().getScreenName() + " - "
						+ quotingTweet.getText());
			}

			public void onException(Exception ex) {
				ex.printStackTrace();
				System.out.println("onException:" + ex.getMessage());
			}
		};
		twitterStream.addListener(listener);

		// user Stream provide real-time updates
		twitterStream.user();
	}

	private static void init() {
		// inverted index
		index = new TreeMap<>();

		// tweets data
		tweets = new HashMap<>();
	}

	@SuppressWarnings("resource")
	private static void connectmongoDB_a(MongoClient mongoClient,
			MongoDatabase database, String dbname, String colname) {
		// Connect to MongoDB
		mongoClient = new MongoClient();
		// Accessing Database
		database = mongoClient.getDatabase(dbname);
		// Retrieving a collection
		collection_a = database.getCollection(colname);
	}

	@SuppressWarnings("resource")
	private static void connectmongoDB_b(MongoClient mongoClient,
			MongoDatabase database, String dbname, String colname) {
		// Connect to MongoDB
		mongoClient = new MongoClient();
		// Accessing Database
		database = mongoClient.getDatabase(dbname);
		// Retrieving a collection
		collection_b = database.getCollection(colname);
	}

	private static void puttweets(String tweet) {

		String[] words = tweet.split(" ");
		for (int i = 0; i < words.length; i++) {
			tweets.put(words[i], tweet);
		}
	}

	private static void invertindex() {
		// build the inverted index
		for (Map.Entry<String, String> entry : tweets.entrySet()) {
			String tweetId = entry.getKey();
			String[] values = entry.getValue().trim().split(" ");

			for (int i = 0; i < values.length; i++) {
				index.putIfAbsent(values[i], new LinkedList<String>());
				LinkedList<String> updatedList = index.get(values[i]);
				updatedList.add(tweetId);
				index.put(values[i], updatedList);
			}
		}
	}

	private static void createDocument(String key, LinkedList<String> value) {
		// Create new document instance
		Document document = new Document();
		// append the data into the document
		document.append("Key", key);
		document.append("Value", value);
		// insert the document into the selected collection
		collection_b.insertOne(document);
	}

}
