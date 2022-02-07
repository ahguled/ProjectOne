import java.util.Scanner
import java.sql.Connection
import java.sql.DriverManager
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.mysql.cj.xdevapi.UpdateStatement
import java.nio.charset.StandardCharsets
//object begin
object projectOne {
    //globals
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/projectone"
    val username = "root"
    val password = "Greenairplane.87"
    var scanner = new Scanner(System.in)
    var connection: Connection = null
    System.setSecurityManager(null)
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\") 
    var exit = false 
    var userMenuEnd = false
    var adminMenuEnd = false
    var correctChoice = false
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    var statement = connection.createStatement()
    var statement1 = connection.createStatement()
   val conf = new SparkConf()
        .setMaster("local") 
        .setAppName("projectOne")  
    val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
    val hiveCtx = new HiveContext(sc)
    import hiveCtx.implicits._ 

//methods/procedures begin
    def addPriv (): Unit={
        adminMenuEnd =false
        correctChoice= false
        println("")
        println("")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("*********                                        GRANT ADMIN ACCESS TO USER                                                             ***********")
        println("*********_______________________________________________________________________________________________________________________________***********")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("")
        println("")
        var validUser = false
        correctChoice = false
        println("Enter the username of the user you want to give admin access to:")
        while (validUser == false){
            scanner.useDelimiter(System.lineSeparator())
            var users1 = scanner.next().toString().toUpperCase()
            var users = users1
            val userRes = statement.executeQuery("Select count(userName) from Users where userName= \""+users1+"\";")
                        while (userRes.next()) {
                        var userRes1 = userRes.getString(1)
                            if (userRes1 == "1") {
                               validUser=true
                               correctChoice = true
                               adminMenuEnd = false
                               println("Admin access given to " + users1)
                               var userUpdate = statement1.executeUpdate( "UPDATE USERS SET user_type = \"ADMIN\" where userName = \""+users+"\";")
                            }else if (users == "0"){
                                println("Returning to Admin Menu")
                                 correctChoice = true
                                adminMenuEnd=false
                                validUser= true
                             } else {
                                correctChoice =false
                             }
                        } 
                 if (correctChoice == false){
                println("Invalid username. Please enter a valid user:")
            }  
        }
    }
    def adminUserDelete(): Unit = {
        adminMenuEnd = false
        var validUser = false
        var correctIn = false
        println("")
        println("")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("*********                                                     DELETE A USER                                                             ***********")
        println("*********_______________________________________________________________________________________________________________________________***********")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("")
        println("")
        while (validUser == false){
            println("Enter username of user to delete the account. Press \"0\" to return to Admin Menu")
            println("")
            scanner.useDelimiter(System.lineSeparator())
            var users1 = scanner.next().toString().toUpperCase()
            val userRes = statement.executeQuery("Select count(userName) from Users where userName= \""+users1+"\";")
                        while (userRes.next()) {
                        var userRes1 = userRes.getString(1)
                            if (userRes1 == "1") {
                                validUser=true
                                correctIn = true
                                correctChoice = false
                                    while (correctChoice == false) {
                                        println("Are you sure you want to delete "+ users1 + "'s account? : Y/N")
                                         var choice = scanner.next().toString().toUpperCase()
                                        if (choice =="Y"){
                                            correctChoice= true
                                            var userUpdate = statement1.executeUpdate( "DELETE from users where userName = \""+users1+"\";")
                                             println( users1+"'s account has been deleted.")
                                             println("Returning to admin menu.")
                                             adminMenuEnd=false
                                        }else if (choice == "N"){
                                            correctChoice= true
                                            println("Returning to admin menu.")
                                            adminMenuEnd=false
                                        }else {
                                            println("Invalid Input. Try Again.")
                                        }
                                }
                            }else if(users1 == "0") {  
                                println("Returning to admin menu.")
                                validUser= true
                                correctIn = true
                                adminMenuEnd=false
                             }else {
                                 println("Invalid Input. Please try again.")
                             }
                        } 
                if (correctIn == false){
                println("Invalid username.")
                }  else if(users1 == "0"){
                    println("Returning to Admin Menu.")
                    adminMenuEnd=false
                }
        }
    }

    def adminUserUpdate(): Unit ={
        adminMenuEnd =false
        var validUser = false
        var correctIn = false
        println("")
        println("")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("*********                                                   CHANGE USER PASSWORD                                                        ***********")
        println("*********_______________________________________________________________________________________________________________________________***********")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("")
        println("")
        println("Enter username of user to change their password. Press \"0\" to return to Admin Menu")
       while (validUser == false){
            scanner.useDelimiter(System.lineSeparator())
            var users1 = scanner.next().toString().toUpperCase()
            val userRes = statement.executeQuery("Select count(userName) from Users where userName= \""+users1+"\";")
                        while (userRes.next()) {
                        var userRes1 = userRes.getString(1)
                            if (userRes1 == "1") {
                               validUser=true
                               correctIn = true
                               println("Enter the temporary new password for " + users1)
                               var newPassword = scanner.next().toString().toUpperCase()
                               var userUpdate = statement1.executeUpdate( "UPDATE USERS SET password= \""+newPassword+"\" where userName = \""+users1+"\";")
                               println("Notify "+ users1+" that their temporary password is " + newPassword + " !!")
                               adminMenuEnd=false
                            }else if(users1 == "0") {  
                                println("Returning to admin menu.")
                                validUser= true
                                correctIn = true
                                adminMenuEnd = false
                             }
                        } 
                if (correctIn == false){
                println("Invalid username. Please enter a valid user:")
            }  
        }
    }
    
    def updateDatabase():Unit ={
        adminMenuEnd=false
                println( "                                          _       ")                       
                println("                               _ __   ___ | | _____ _ __ ___   ___  _ __  ")
                println("                              | '_ \\ / _ \\| |/ / _ \\ '_ ` _ \\ / _ \\| '_ \\ ")
                println("                              | |_) | (_) |   <  __/ | | | | | (_) | | | |")
                println("                              | .__/ \\___/|_|\\_\\___|_| |_| |_|\\___/|_| |_|")
                println("                              |_|                                 ")
        println("Updating database with new Pokemon. Please hold .....")
        var output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("Inputs/pokedex_(Update_05.20).csv")
    
            output.createOrReplaceTempView("temp_poke")
            println("                                                      .................................. almost.")
            hiveCtx.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
            hiveCtx.sql("SET hive.enforce.bucketing=false")
            hiveCtx.sql("SET hive.enforce.sorting=false")
            hiveCtx.sql("CREATE TABLE IF NOT EXISTS pokedex(index_number INT,pokedex_number INT,name STRING,german_name STRING,japanese_name STRING,generation INT,status STRING,species STRING,type_number INT,type_2 STRING,height_m DOUBLE,weight_kg DOUBLE,abilities_number INT,ability_1 STRING,ability_2 STRING,ability_hidden STRING,total_points INT,hp INT,attack INT,defense INT,sp_attack INT,sp_defense INT,speed INT,catch_rate INT,base_friendship INT,base_experience INT,growth_rate STRING,egg_type_number INT,egg_type_1 STRING,egg_type_2 STRING,percentage_male DOUBLE,egg_cycles INT,against_normal INT,against_fire INT,against_water INT,against_electric INT,against_grass INT,against_ice INT,against_fight INT,against_poison INT,against_ground INT,against_flying INT,against_psychic INT,against_bug INT,against_rock INT,against_ghost INT,against_dragon INT,against_dark INT,against_steel INT,against_fairy INT) partitioned BY (type_1 string) clustered by (generation) sorted by (name) into 8 buckets" )
            hiveCtx.sql("INSERT OVERWRITE TABLE pokedex Select * FROM temp_poke")
            hiveCtx.sql("Select * from pokedex order by pokedex_number DESC limit 10")
            println("Done. Database updated.")

            adminMenuEnd = false
    }
    def deleteUserAccount(username1: String): Unit = {
        correctChoice = false
        userMenuEnd=false
        scanner.useDelimiter(System.lineSeparator())
        println("")
        println("")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("*********                                                   SAD TO SEE YOU GO!!                                                         ***********")
        println("*********_______________________________________________________________________________________________________________________________***********")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("")
        println("")
        println("Are you sure you want to go? You will no longer be able to access the Eorzean database")
        println("\"Y\" for Yes, \"N\" for No")
        var choice = scanner.next().toString().toUpperCase()
        while (correctChoice == false) 
                if (choice == "Y"){
                    var delete = statement.executeUpdate("Delete from users where username = \""+username1+"\";")
                    println("Your account has been deleted. Come back soon!")
                    println("Returning you to Main Menu")
                    correctChoice = true
                    userMenuEnd = true
                } else if (choice == "N"){
                    correctChoice = true
                    userMenuEnd = false
                    println("Returning to User Menu !")
                }else {
                    println("Invalid input please try again:")
                    choice = scanner.next().toString().toUpperCase()
                }
    }
    def updateUserAccount(username1: String):Unit= {
        userMenuEnd=false
        correctChoice = false
        var userNameNotTaken=false
        var newUsername1 = ""
        println("")
        println("")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("*********                                                   CHANGE CREDENTIALS                                                          ***********")
        println("*********_______________________________________________________________________________________________________________________________***********")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("")
        println("")
        while (correctChoice == false) {
            println("Update user account ? Press \"0\" to return to User Menu")
            println("1.) Change username")
            println("2.) Change password")
            scanner.useDelimiter(System.lineSeparator())
            var choice = scanner.next().toString().toUpperCase()
                if (choice == "1"){
                     println("Enter your new username")
                    newUsername1 = scanner.next().toString().toUpperCase()
                     while (userNameNotTaken == false){
                        val userRes = statement.executeQuery("Select count(userName) from Users where username= \""+newUsername1+"\";")
                        while (userRes.next()) {
                            var userRes1 = userRes.getString(1)
                                 if (userRes1 == "1") {
                                    println("Sorry!! That username is taken. Try again: ")
                                    newUsername1 = scanner.next().toString().toUpperCase()
                                    userNameNotTaken = false
                                }else if (newUsername1 == "" || newUsername1 == " " || newUsername1 == "   "|| newUsername1 == "     "){
                                    println("Username cannot be blank try again.")
                                 }else {
                                    userNameNotTaken = true
                                    correctChoice = true
                                    userMenuEnd=true
                                }
                         }
                     }
                    if (userNameNotTaken == true ){
                        var newUsername = newUsername1
                        var userUpdate = statement1.executeUpdate( "UPDATE USERS SET username = \""+newUsername+"\" where userName = \""+username1+"\";")
                        println("Username set as "+newUsername1+ " You must sign back in with new credentials at Main Menu.")
                     } else {}
                }else if (choice =="2"){
                    println("Enter your new password")
                    var newPassword1 = scanner.next().toString().toUpperCase()
                    var newPassword = newPassword1
                    var pwUpdate = statement.executeUpdate( "UPDATE USERS SET password = \""+newPassword+"\" where userName = \""+username1+"\";")
                    println("You must sign back in with new credentials at Main Menu.")
                    userMenuEnd = true
                    correctChoice = true
                } else if (choice == "0"){
                    correctChoice = true
                    userMenuEnd = false
                    println("Returning to the User Menu")
                } else {
                    println("Invalid input please try again!")
                }
        }
    }
    def checkDatabase ():Unit= {
        userMenuEnd= false
        correctChoice = false
        println("")
        println("")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("*********                                                    POKEMON DATABASE                                                           ***********")
        println("*********_______________________________________________________________________________________________________________________________***********")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("")
        println("")
        println("Please hold while we load the database <3 ..... Enjoy this random selection of pokemon while you wait.")
        var poke= hiveCtx.sql("Select name,generation,species,type_1,type_2, ability_1,ability_2,ability_hidden from pokedex sort by rand() Limit 10")
        poke.show()
        while (correctChoice == false) {
            println("Choose one of the Questions below for interesting pokemon facts. Use \"0\" for User Menu")
            println("1.) What pokemon gives most power for least effort?")
            println("2.) What are some of the latest pokemon?")
            println("3.) What are the strongest fairy pokemon?")
            println("4.) How many pokemon currently exist and what genration are we in?" )
            println("5.) What are the Top 10 Smallest and Top 10 Largest Pokemon(Ordered by Height Descending)?")
            println("6.) Search all stats for pokemon with specific ability or for a specific pokemon?")
            println("7.) Sign out and Exit")
            scanner.useDelimiter(System.lineSeparator())
            var choice = scanner.next().toString()
                if (choice == "1"){
                    val summary = hiveCtx.sql("SELECT name,pokedex_number,species,type_1,type_2,ability_1,ability_2,ability_hidden, total_points, catch_rate FROM pokedex where total_points >=400 AND catch_rate >= 150  order by total_points DESC LIMIT 20 ")
                    summary.show()
                }else if (choice == "2"){

                    val summary = hiveCtx.sql("SELECT name,pokedex_number,species,type_1,type_2, ability_1,ability_2,ability_hidden FROM pokedex WHERE generation in (Select MAX(generation) from pokedex) sort by rand()  LIMIT 20")
                    summary.show()       
                }else if(choice == "3"){
                     val summary = hiveCtx.sql("SELECT name,pokedex_number,status,species,type_1,type_2,ability_1,ability_2,ability_hidden,total_points,hp,attack,defense,sp_attack,sp_defense,speed FROM pokedex WHERE type_2 = \"Fairy\" OR type_1= \"Fairy\"  order by total_points DESC LIMIT 20 ")
                     summary.show()
                } else if ( choice == "4"){
                     val summary = hiveCtx.sql("SELECT COUNT(DISTINCT pokedex_number) Total_Pokemon,MAX(generation) Current_generation from pokedex")
                     summary.show()
                }else if (choice == "5"){
                     val summary1 = hiveCtx.sql(" CREATE VIEW IF NOT EXISTS small AS SELECT name,generation,status,species,type_1,type_2,height_m,weight_kg FROM pokedex ORDER BY height_m ASC Limit 10 ")
                     var summary2 = hiveCtx.sql("CREATE VIEW IF NOT EXISTS big AS SELECT name,generation,status,species,type_1,type_2,height_m,weight_kg FROM pokedex ORDER BY height_m DESC Limit 10 ")
                     var summary = hiveCtx.sql("Select * from small UNION ALL (SELECT * from big) order by height_m DESC")
                      summary.show()
                }else if (choice == "6"){
                     println("What skill or pokemon are you looking for?")
                     var skill =scanner.next().toString()
                     val summary = hiveCtx.sql("SELECT name,generation,status,species,type_1,type_2,height_m,weight_kg,ability_1,ability_2,ability_hidden,total_points,hp,attack,defense,sp_attack,sp_defense,speed FROM pokedex where name = \""+skill+"\" OR ability_1 = \""+ skill +"\" OR ability_2 = \""+ skill +"\" OR ability_hidden = \""+ skill +"\" ")
                      summary.show()
                }else if (choice == "7"){
                     correctChoice = true
                     exit = true
                     userMenuEnd = true
                }else if (choice == "0"){
                     correctChoice = true
                    println("Returning to User options.")
                }else {
                    println("Invalid input please select from listed option or press 0 to return to User Menu")
                }
        }    
    }
    def adminMenu(username1: String):Unit= {
        correctChoice = false
        adminMenuEnd=false
        while (adminMenuEnd == false) {
        println("")
        println("")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("*********                                                          ADMIN MENU                                                           ***********")
        println("*********_______________________________________________________________________________________________________________________________***********")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("")
        println("")
        correctChoice = false
            while (correctChoice == false) {
            println("Select an option below: Use \"0\" for normal user menu actions")
            println("1.) Update Database")
            println("2.) Update or delete a User account")
            println("3.) Add Admin privledges to an account")
            println("4.) Sign out and Exit")
            scanner.useDelimiter(System.lineSeparator())
            var choice = scanner.next().toString().toUpperCase()
                if ( choice == "1") {
                    correctChoice = true
                    adminMenuEnd=true
                    updateDatabase()
                }else if (choice == "2"){
                    var adChoice= false
                    correctChoice = true
                    adminMenuEnd=true
                        while (adChoice == false){
                            println("Select an option:, select \"0\" to return to Admin Menu menu")
                            println("1.) Update a user password or username")
                            println("2.) Delete a User account")
                            scanner.useDelimiter(System.lineSeparator())
                            var adminChoice = scanner.next().toString().toUpperCase()
                                if (adminChoice == "1"){
                                    adminUserUpdate()
                                    adChoice=true
                                } else if ( adminChoice=="2"){
                                    adminUserDelete()
                                    adChoice=true
                                } else if (adminChoice == "0"){
                                    println("Returning to previous menu.")
                                    adChoice = true
                                    correctChoice=false
                                    adminMenuEnd=false
                                }else {
                                    println("Invalid input. Please try again")
                                }
                        }
                } else if (choice =="3"){
                    correctChoice = true
                    addPriv()
                    adminMenuEnd= true
                }else if (choice =="0") {
                    correctChoice=true
                    adminMenuEnd = true
                    userMenuEnd= false
                    println("Normal User functions enabled.")
                    userMenu(username1)
                } else if( choice == "4") {
                    adminMenuEnd = true
                    exit=true
                    correctChoice = true
                    println("Signed out. Goodbye admin " + username1)
                }else {
                    println("Invalid input try again.")
                }
            }
        }
    }
    def userMenu (username1: String):Unit= {
        userMenuEnd = false
        while (userMenuEnd == false) {
            println("")
            println("")
            println("***************************************************************************************************************************************************")
            println("***************************************************************************************************************************************************")
            println("*********                                                    Gotta Catch Em All                                                         ***********")
            println("*********_______________________________________________________________________________________________________________________________***********")
            println("***************************************************************************************************************************************************")
            println("***************************************************************************************************************************************************")
            println("")
            println("")
            correctChoice = false
            while (correctChoice == false){
                println("Select an option below : ")
                println("1.) Check Database")
                println("2.) Change User Account Information")
                println("3.) Delete  your User Account")  
                println("4.) Sign Out and Exit")
                println("")
                scanner.useDelimiter(System.lineSeparator())
                var choice = scanner.next().toString().toUpperCase()
                 if (choice == "0") {
                    userMenuEnd=true
                    correctChoice = true
                    println("Returning to Main Menu.")
                    println("")
                    println("")
                }else if ( choice == "1") {
                    correctChoice = true
                    userMenuEnd = true
                    checkDatabase()
                } else if (choice == "2") {
                    correctChoice = true
                    userMenuEnd=true
                    updateUserAccount(username1)
                }else if ( choice == "3"){
                    correctChoice = true
                    userMenuEnd = true
                    deleteUserAccount(username1)
                } else if (choice == "4"){
                    userMenuEnd = true
                    correctChoice=true
                    exit = true
                    println("Signed Out. Goodbye " + username1+ "!!")
                }else {
                    println("Invalid input select from listed options. Use \"0\" to return to previous menu")
                    println("")
                }
            }    
        }
    }
    def adminLogIn()= {
    var aliEnd = false
    var userNameValid = false
    var passwordValid = false
    var signin= false
    var username1 = ""
    var signedIn = false
        while (aliEnd== false){
            println("")
            println("")
            println("***************************************************************************************************************************************************")
            println("***************************************************************************************************************************************************")
            println("*********                                               Log-In for Admin Access Menu                                                    ***********")
            println("*********_______________________________________________________________________________________________________________________________***********")
            println("***************************************************************************************************************************************************")
            println("***************************************************************************************************************************************************")
            println("")
            println("")
             while (userNameValid == false && signin == false)  { 
                println("Please enter your username: ")
                scanner.useDelimiter(System.lineSeparator())
                 username1 = scanner.next().toString().toUpperCase()
                    if (username1 == "0"){
                        println("Returning to main menu")
                        println("")
                        userNameValid = true 
                        aliEnd = true 
                        signin = true
                        passwordValid=true
                    }else{
                    val userRes = statement.executeQuery("Select userName from Users where user_type =\"ADMIN\" ;")
                        while (userRes.next() && signin == false) {
                        var userRes1 = userRes.getString(1)
                            if (userRes1 == username1) {
                               userNameValid = true
                                signin = true
                            }else {
                                userNameValid= false
                                 signin = false
                             }
                        }  
                        if (signin == false) {
                            println("Sorry that Username is not a valid admin, try again or return to Main Menu")
                            println("To return to Main Menu press \"0\" ") 
                        } 
                    }
                }
                while (passwordValid == false) {
                    println("")
                    println("Please enter your passsword")
                    scanner.useDelimiter(System.lineSeparator())
                    var password1 = scanner.next().toString().toUpperCase()
                        if (password1 == "0"){
                            println("Returning to main menu")
                            println("")
                            passwordValid  = true 
                            aliEnd = true 
                        }else{
                        val pwRes = statement.executeQuery("Select password from Users where userName =\""+username1+"\";")
                            while (pwRes.next()) {
                            var pwRes1 = pwRes.getString(1)
                                if (pwRes1 == password1) {
                                    passwordValid = true
                                    signedIn = true
                                 } else {
                                     println("")
                                     println("Sorry, that password doesn't match. Retry or contact another admin.")
                                     println("To return to Main Menu press \"0\" ")
                                    passwordValid = false
                                }
                             }
                         }    
                 }
             if (userNameValid == true && passwordValid == true && signedIn == true){
             aliEnd = true 
             println("")
             println("Admin access established: Welcome Admin: " + username1)
             adminMenu(username1)
            }
        }     
    }
    def userLogIn() = {
        var userNameValid = false
        var logInEnd= false
        var passwordValid = false
        var signin= false
        var username1 = ""
        var password1 = ""
        var signedIn = false
        while (logInEnd == false) {
        println("")
        println("")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("*********                                                        User Sign-In                                                           ***********")
        println("*********_______________________________________________________________________________________________________________________________***********")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("")
        println("")
        println("Hello, ")
            while (userNameValid == false && signin == false)  { 
                println("Please enter your username: ")
                scanner.useDelimiter(System.lineSeparator())
                 username1 = scanner.next().toString().toUpperCase()
                    if (username1 == "0"){
                        println("Returning to Main Menu")
                        println("")
                        userNameValid = true 
                        logInEnd = true 
                        signin = true
                        passwordValid = true
                    }else{
                    val userRes = statement.executeQuery("Select userName from Users;")
                        while (userRes.next() && signin == false) {
                        var userRes1 = userRes.getString(1)
                            if (userRes1 == username1) {
                               userNameValid = true
                                signin = true
                            }else {
                                userNameValid= false
                                 signin = false
                             }
                        } 
                        if (signin == false) {
                            println("Sorry that username is invalid try again or return to Main Menu and create an account")
                            println("To return to Main Menu press \"0\" ") 
                        } else{} 
                    } 
             }
         while (passwordValid == false) {
            println("")
            println("Please enter your passsword")
            scanner.useDelimiter(System.lineSeparator())
            password1 = scanner.next().toString().toUpperCase()
                 if (password1 == "0"){
                     println("Returning to Main Menu")
                    passwordValid  = true 
                    logInEnd = true 
                }else{
                val pwRes = statement.executeQuery("Select password from Users where userName =\""+username1+"\";")
                    while (pwRes.next()) {
                    var pwRes1 = pwRes.getString(1)
                        if (pwRes1 == password1) {
                            passwordValid = true
                            signedIn = true
                        } else {
                            println("")
                            println("Sorry, that password doesn't match. Retry or contact an admin.")
                            println("To return to Main Menu press \"0\" ")
                            passwordValid = false
                        }
                    }
                 }
             }
         if (userNameValid == true && passwordValid == true && signedIn == true){
             logInEnd = true 
             println("You now have access to the Pokédex, " + username1)
             userMenu(username1)
         } else if (username1 == "0"|| password1  == "0"){
             passwordValid  = true 
             logInEnd = true 
             userNameValid = true
             signin = true
         }
        } 
    }
    def createNewUser () = {
        var userName1 = ""
        scanner.useDelimiter(System.lineSeparator())
        println("")
        println("") 
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("*********                                                Create New User: Welcome :)                                                    ***********")
        println("*********_______________________________________________________________________________________________________________________________***********")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("")
        println("") 
        var userNameNotTaken = false
        var passwordInputGood = false
        println("Enter your legal first name: ")
        var userFirstName1 = scanner.next().toString().toUpperCase()
        var userFirstName = userFirstName1
        println("Enter your legal last name: ")
        var userLastName1 = scanner.next().toString().toUpperCase()
        var userLastName = userLastName1
        println("Enter a username that will be used to access database: (If the username is taken you will be asked to choose another.)")
        userName1 = scanner.next().toString().toUpperCase()
             while (userNameNotTaken == false){
                 val userRes = statement.executeQuery(" Select count(userName) from Users where username= \""+userName1+"\" ;")
                    while (userRes.next()) {
                    var userRes1 = userRes.getString(1)
                        if (userRes1 == "1") {
                            println("Sorry!! That username is taken. Try again: ")
                             userName1 = scanner.next().toString().toUpperCase()
                             userNameNotTaken = false
                        }else if (userName1 == "" || userName1 == " "){
                            println("Username cannot be blank try again.")
                        }else {
                             userNameNotTaken = true
                        }
                    }
                }
        var userName = userName1
        println("Enter your password that will be used to access database")
        var password1 = scanner.next().toString().toUpperCase()
             while (passwordInputGood == false){
                if (password1 == "" || password1 == " "){
                    println("Password cannot be blank")
                    password1= scanner.next().toString().toUpperCase()
                 }else if (password1.length <= 3) {
                     println("Are you sure you want a short password? (It may be less secure)")
                     println("Y for yes, N for No")
                     var sp = scanner.next.toString().toUpperCase()
                     var shortPw = false
                     while (shortPw == false){
                        if (sp =="Y" || sp == "YES") {
                            passwordInputGood = true
                            shortPw = true
                        }else if (sp == "N"|| sp == "NO") {
                            println("Enter a different password")
                            password1= scanner.next().toString().toUpperCase()
                            shortPw = true
                        }else {
                            println("Invalid input try again: \"Y/N\"")
                            sp = scanner.next.toString().toUpperCase()
                            shortPw = false
                        }
                     }
                 }else {
                     passwordInputGood = true
                 }
            }
        var password = password1
        val addedBankUsser = statement.executeUpdate(
             "Insert Into Users(first_name,last_name,username,password,user_type) values (\"" + userFirstName + "\", \"" + userLastName + "\", \"" + userName + "\",\"" +password+ "\",\"BASIC\");"
             )
             println("Welcome to the database " + userName + "!!!" )
             println("Returning to Main Menu")
    }
    def mainMenu() = { 
        var option = ""
        while (exit == false) {
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("*********                                           Welcome to the Pokemon Database                                                     ***********")
        println("*********_______________________________________________________________________________________________________________________________***********")
        println("***************************************************************************************************************************************************")
        println("***************************************************************************************************************************************************")
        println("")
        println("")
        println("    SELECT AN OPTION BELOW BY PRESSING \"1-3\" FOR ACCESS TO DATABASE (FOR ADMIN ACCESS PRESS \"0\"):")
        println("      1.) User Sign In")
        println("      2.) Create New User")
        println("      3.) Exit")
        scanner.useDelimiter(System.lineSeparator())
         option = scanner.next().toString()
        var correctChoice = false
            while (correctChoice ==  false){
                 if (option == "1"){
                    correctChoice = true
                    userLogIn()
            
                } else if(option == "2") {
                    correctChoice = true
                    createNewUser()
            
                } else if (option == "3") {
                    correctChoice = true
                    println("")
                    println("")
                    println("                  ***************************************** Closing Database. Goodbye !!!! *****************************************")
                    
                    exit = true

                } else if (option == "0") {
                    correctChoice = true
                    adminLogIn()    
                 }else {
                    println("Try again, you used an invalid input.")
                    option = scanner.next().toString()
                 }
             }
         } 
         if (exit == true){
              println("")
                    println("")
              println("")
                    println("")
                     println("")
                    println("")
           
println("                                                                  ,'\\")
println("                                    _.----.        ____         ,'  _\\   ___    ___     ____")
println("                                _,-'       `.     |    |  /`.   \\,-'    |   \\  /   |   |    \\  |`.")
println("                                \\      __    \\    '-.  | /   `.  ___    |    \\/    |   '-.   \\ |  |")
println("                                 \\.    \\ \\   |  __  |  |/    ,','_  `.  |          | __  |    \\|  |")
println("                                   \\    \\/   /,' _`.|      ,' / / / /   |          ,' _`.|     |  |")
println("                                    \\     ,-'/  /   \\    ,'   | \\/ / ,`.|         /  /   \\  |     |")
println("                                     \\    \\ |   \\_/  |   `-.  \\    `'  /|  |    ||   \\_/  | |\\    |")
println("                                      \\    \\ \\      /       `-.`.___,-' |  |\\  /| \\      /  | |   |")
println("                                       \\    \\ `.__,'|  |`-._    `|      |__| \\/ |  `.__,'|  | |   |")
println("                                        \\_.-'       |__|    `-._ |              '-.|     '-.| |   |")
println("                                                                `'                            '-._|")
                try{
             Thread.sleep(10000)
              println("")
                    println("")
println("                                                          .\"-,.__ ")
println("                                                        `.     `.  ,")
println("                                                     .--'  .._,'\"-\' `.")
println("                                                    .    .'         `\'")
println("                                                    `.   /          ,\'")
println("                                                      `  \'--.   ,-\"'")
println("                                                       `\"`   |  ")
println("                                                          -. \\, |")
println("                                                           `--Y.\'      ___.")
println("                                                                \\     L._, ")
println("                                                      _.,        `.   <  <\\                _")
println("                                                    ,\' \'           `, `.   | \\            ( `")
println("                                                 ../, `.            `  |    .\\`.           \\ \\_")
println("                                               ,\' ,..  .           _.,\'    ||\\l            )  \'\".")
println("                                               , ,\'   \\           ,\'.-.`-._,\'  |           .  _._`.")
println("                                             ,\' /      \\ \\        `\' \' `--/   | \\          / /   ..")
println("                                           .'  /        \\ .         |\\__ - _ ,\'` `        / /     `.`.")
println("                                           |  '          ..         `-...-\"  |  `-'      / /        . `.")
println("                                           | /           |L__           |    |          / /          `. `.")
println("                                          , /            .   .          |    |         / /             ` `")
println("                                         / /          ,. ,`._ `-_       |    |  _   ,-\' /               ` \\")
println("                                        / .           \"`_/. `-_ \\_,.  ,\'    +-\' `-'  _,        ..,-.    `. \\")
println("                                       .  '         .-f    ,'   `    '.       \\__.---'     _   .'   '     \\ \\")
println("                                       ' /          `.'    l     .' /          \\..      ,_|/   `.  ,'`     L`")
println("                                       |\'      _.-\"\"` `.    \\ _,\'  `            \\ `.___`.\'\"`-.  , |   |    | \\")
println("                                       ||    ,\'      `. `.   \'       _,...._        `  |    `/ \'  |   \'     .|")
println("                                       ||  ,\'          `. ;.,.---\' ,\'       `.   `.. `-\'  .-\' /_ .\'    ;_   ||")
println("                                       || \'              V      / /           `   | `   ,\'   ,\' \'.    !  `. ||")
println("                                       ||/            _,-------7 \'              . |  `-\'    l         /    `||")
println("                                       . |          ,\' .-   ,\' ||               | .-.        `.      .\'     ||")
println("                                        `\'        ,\'    `\".'    |               |    `.        \'. -.\'       `\'")
println("                                                 /      ,\'      |               |,\'    \\-.._,.\'/\'")
println("                                                 .     /        .               .       \\    .\'\'")
println("                                               .`.    |         `.             /         :_,\'.\'")
println("                                                 \\ `...\\   _     ,\'-.        .'         /_.-\'")
println("                                                  `-.__ `,  `\'   .  _.>----\'\'.  _  __  /")
println("                                                       .\'        /\"\'          |  \"\'   \'_")
println("                                                      /_|.-\'\\ ,\".             \'.\'`__\'-( \\")
println("                                                        / ,\"\'\"\\,'               `/  `-.|  ")
             } catch {
                 case i: java.lang.InterruptedException =>{
                     
                 }
             }
         }       
    } 
        def main(args: Array[String]): Unit  = {
        try {     
            mainMenu()
        } catch {                 
            case e: Exception => {
                 e.printStackTrace                    
            }   
        }
        connection.close()
        scanner.close()
        sc.stop()
    } 
} 