package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.text.MessageFormat;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;
    //TODO> Ticket: User Management - do the necessary changes so that the sessions collection
    //returns a Session object
    private final MongoCollection<Session> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());
        //TODO> Ticket: User Management - implement the necessary changes so that the sessions
        // collection returns a Session objects instead of Document objects.
        sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        //TODO > Ticket: Durable Writes -  you might want to use a more durable write concern here!
        usersCollection.withWriteConcern(WriteConcern.MAJORITY);
        //TODO > Ticket: Handling Errors - make sure to only add new users
        // and not users that already exist.
        Boolean status;
        User check = getUser(user.getEmail());

        if(check != null) {
            throw new IncorrectDaoOperation("User already exists.");
        } else {
            status = true;
        }

        usersCollection.insertOne(user);

        User temp_user = usersCollection
                .find(
                        new Document("email", user.getEmail()))
                .first();

        System.out.println(temp_user);
        return status;

    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
        //TODO> Ticket: User Management - implement the method that allows session information to be
        // stored in it's designated collection.
        Session session = new Session();
        session.setUserId(userId);
        session.setJwt(jwt);

        Document queryFilter = new Document();
        queryFilter.put("user_id", userId);  //_id to user_id cause bson@

        if (sessionsCollection.find(queryFilter).iterator().hasNext()) {
            deleteUserSessions(userId);
        }

        sessionsCollection.insertOne(session);
        return true;
        //TODO > Ticket: Handling Errors - implement a safeguard against
        // creating a session with the same jwt token.
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        User user = null;
        //TODO> Ticket: User Management - implement the query that returns the first User object.
        Document queryFilter = new Document("email", email);
        user = usersCollection.find(queryFilter).first();
        System.out.println(user);
        return user;
    }
    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        //TODO> Ticket: User Management - implement the method that returns Sessions for a given
        // userId
        Session session = null;
        Document queryFilter = new Document("user_id" ,userId);
        session = sessionsCollection.find(queryFilter).first();
        return session;
    }

    public boolean deleteUserSessions(String userId) {
        //TODO> Ticket: User Management - implement the delete user sessions method
        Document queryFilter = new Document();
        queryFilter.put("user_id", userId);

        sessionsCollection.deleteMany(queryFilter);

        return true;
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        // remove user sessions
        //TODO> Ticket: User Management - implement the delete user method
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions.
        Document queryFilter = new Document();
        queryFilter.put("email", email);

        usersCollection.deleteOne(queryFilter);
        deleteUserSessions(email);

        return true;
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        //TODO> Ticket: User Preferences - implement the method that allows for user preferences to
        // be updated.
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions when updating an entry.
        Boolean status = false;

        if(userPreferences == null){
            throw new IncorrectDaoOperation("Perferences can't be null");
        }

        try {
            User user = getUser(email);
            if(user != null){
                usersCollection.updateOne(eq("email", email), set("preferences", userPreferences));
            }
            status = true;
        } catch (Exception e) {
            // throw new IncorrectDaoOperation("User already exists.");
            e.printStackTrace();
        }

        Document queryFilter = new Document("email", email);
        User user = usersCollection.find(queryFilter).first();
        System.out.println(user.getPreferences().toString());

        return status;
    }

}
