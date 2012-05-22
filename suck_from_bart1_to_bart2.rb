#!/usr/bin/env ruby
require "mysql"
require "yaml"
require "logger"

# Require a user to input 3 parameters:
#   1: true/false ::if this is the first call, true else false
#   2: first position ::to start from
#   3: range  ::total patients on this call
#   4: insert reason for starting art as well (1-yes; 0-no)

if ARGV.length < 4
  print "\nSorry, this script expects 3 arguments in this order \n\n\t./filename.rb arg1 arg2 arg3 [arg4]\n\n" + 
    "where: \n\targ1\t: true/false - meaning this is the first file in a series of calls or not\n\t" + 
    "arg2\t: start patient position\n\t" + 
    "arg3\t: range of patients on this run\n\t" + 
    "arg4\t: insert reason for starting art as well (1-yes; 0-no)" +
    "[arg5]\t: optional log file\n\n"
  exit
end

log = Logger.new('log/log.txt')

host = YAML::load_file('database.yml')['bart1_migration_source']['host']
user = YAML::load_file('database.yml')['bart1_migration_source']['username']
pass = YAML::load_file('database.yml')['bart1_migration_source']['password']
db = YAML::load_file('database.yml')['bart1_migration_source']['database']

dest_host = YAML::load_file('database.yml')['bart2_migration_destination']['host']
dest_user = YAML::load_file('database.yml')['bart2_migration_destination']['username']
dest_pass = YAML::load_file('database.yml')['bart2_migration_destination']['password']
dest_db = YAML::load_file('database.yml')['bart2_migration_destination']['database']

con = Mysql.connect(host, user, pass, db)

dest_con = Mysql.connect(dest_host, dest_user, dest_pass, dest_db)

# One-off initialisation stuff when ARGV[0] = true
if ARGV[0] == "true" || ARGV[0] == "1"   
  `mysql --user=#{dest_user} --password=#{dest_pass} -e DROP DATABASE #{dest_db};`

  sleep(1)

  `mysql --user=#{dest_user} --password=#{dest_pass} -e CREATE DATABASE #{dest_db};`

  print "# loading schema/schema.sql file\n"
  `mysql --host=#{dest_host} --user=#{dest_user} --password=#{dest_pass} #{dest_db} < schema/schema.sql`

  print "# loading schema/patient_report.sql file\n"
  `mysql --host=#{dest_host} --user=#{dest_user} --password=#{dest_pass} #{dest_db} < schema/patient_report.sql`

  print "# loading schema/patient_report_details.sql file\n"
  `mysql --host=#{dest_host} --user=#{dest_user} --password=#{dest_pass} #{dest_db} < schema/patient_report_details.sql`

  print "# loading schema/defaults.sql file\n"
  `mysql --host=#{dest_host} --user=#{dest_user} --password=#{dest_pass} #{dest_db} < schema/defaults.sql`
 
  functions = Dir.glob("initializers/*")

  functions.each do |file|  
    print "# loading #{file} function file\n"
    `mysql --host=#{host} --user=#{user} --password=#{pass} #{db} < #{file}`
  end

  triggers = Dir.glob("schema/triggers/*")

  triggers.each do |file|  
    print "# loading #{file} trigger file\n"
    `mysql --host=#{dest_host} --user=#{dest_user} --password=#{dest_pass} #{dest_db} < #{file}`
  end

  patients = con.query("SELECT (MAX(patient_id) + 1) lastid FROM patient")

  patients = patients.fetch_hash

  lastid = patients["lastid"].to_i

  begin  
    p = dest_con.query("DROP TABLE IF EXISTS `users_mapping`")
    p = dest_con.query("DROP TABLE IF EXISTS `obs_edit_audit`")
    
    p = dest_con.query("CREATE TABLE `users_mapping` (" + 
        "`bart1_user_id` INT, `bart2_user_id` INT, `bart1_patient_id` INT, `bart2_person_id` INT ) ENGINE = InnoDB")
  
    p = dest_con.query("CREATE TABLE `obs_edit_audit` (`patient_id` INT, `obs_id` INT, `drug_id` INT) ENGINE = InnoDB") 
  
    p = dest_con.query("DELETE FROM relationship")
  rescue Mysql::Error => e
    puts "?? Error #{e.errno}: #{e.error}\n"
  end

  users = con.query("SELECT user_id FROM users") 

  p = dest_con.query("SET FOREIGN_KEY_CHECKS=0")  

  p = dest_con.query("START TRANSACTION")  

  users.each_hash do |user|
    print "# importing user with id #{user["user_id"]} to #{lastid}\n"
  
    begin
      p = dest_con.query("INSERT INTO person SELECT #{lastid}, NULL, NULL, NULL, NULL, NULL, " + 
          "NULL, `#{db}`.`users`.`creator`, `#{db}`.`users`.`date_created`, `#{db}`.`users`.`changed_by`, " + 
          "`#{db}`.`users`.`date_changed`, `#{db}`.`users`.`voided`, `#{db}`.`users`.`voided_by`, `#{db}`.`users`.`date_voided`, " + 
          "`#{db}`.`users`.`void_reason`, (SELECT UUID()) FROM `#{db}`.`users` WHERE `#{db}`.`users`.`user_id` = #{user["user_id"]}")
    
      p = dest_con.query("INSERT INTO users_mapping VALUES(#{user["user_id"]}, #{lastid}, NULL, NULL)")
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}\n"
    end
    
    lastid = lastid + 1
  end

  new_users = dest_con.query("SELECT person_id, COALESCE((SELECT bart2_user_id FROM users_mapping WHERE " + 
      "bart1_user_id = creator),'') creator, COALESCE((SELECT bart2_user_id FROM users_mapping WHERE bart1_user_id = changed_by),'') " + 
      "changed_by, COALESCE((SELECT bart2_user_id FROM users_mapping WHERE bart1_user_id = voided_by),'') voided_by FROM person")

  new_users.each_hash do |user|
    print "# updating user with id #{user["person_id"]}\n"
  
    begin
      p = dest_con.query("UPDATE person SET creator = '#{user["creator"]}'" + 
          "#{ (user["changed_by"].to_i > 0 ? (", changed_by = '" + user["changed_by"] + "'") : "") }  " + 
          "#{ (user["voided_by"].to_i > 0 ? (", voided_by = '" + user["voided_by"] + "'") : "") }  " + 
          "WHERE person_id = #{user["person_id"]}")
    
      p = dest_con.query("INSERT INTO person_name SELECT NULL, NULL, #{lastid}, NULL, `#{db}`.`users`.`first_name`, " + 
          "`#{db}`.`users`.`middle_name`, NULL, `#{db}`.`users`.`last_name`, NULL, NULL, NULL, #{user["creator"]}," + 
          " `#{db}`.`users`.`date_created`, `#{db}`.`users`.`voided`, #{(user["voided_by"].to_i > 0 ? 
        user["voided_by"] : "NULL")}, `#{db}`.`users`.`date_voided`, `#{db}`.`users`.`void_reason`, " + 
          "#{(user["changed_by"].to_i > 0 ? (user["changed_by"]) : "NULL")}, `#{db}`.`users`.`date_changed`, " +         
          "(SELECT UUID()) FROM `#{db}`.`users` WHERE `#{db}`.`users`.`user_id` = (SELECT bart1_user_id FROM users_mapping WHERE " + 
          "bart2_user_id = #{user["person_id"]})")
    
      p = dest_con.query("INSERT INTO users SELECT #{user["person_id"]}, NULL, `#{db}`.`users`.`username`, `#{db}`.`users`.`password`, " + 
          "`#{db}`.`users`.`salt`, `#{db}`.`users`.`secret_question`, `#{db}`.`users`.`secret_answer`, #{user["creator"]}, " + 
          "`#{db}`.`users`.`date_created`, #{(user["changed_by"].to_i > 0 ? (user["changed_by"]) : "NULL")}, " + 
          "`#{db}`.`users`.`date_changed`, #{user["person_id"]}, NULL, NULL, NULL, NULL, " + 
          "(SELECT UUID()), NULL FROM `#{db}`.`users` WHERE `#{db}`.`users`.`user_id` = (SELECT bart1_user_id FROM users_mapping WHERE " + 
          "bart2_user_id = #{user["person_id"]})")
    
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}\n"
    
    end
  end
  
  p = dest_con.query("COMMIT")  

end

# GENERAL
people = con.query("SELECT patient_id FROM patient ORDER BY date_created DESC LIMIT #{ARGV[1]}, #{ARGV[2]}") 

p = dest_con.query("SET UNIQUE_CHECKS=0")
p = dest_con.query("SET FOREIGN_KEY_CHECKS=0")  

starttime = Time.now

log.debug "Actual patient stuff started at: #{starttime}"

pos = 0
people.each_hash do |person|
  
  p = dest_con.query("START TRANSACTION")  

  pos = pos + 1  
  puts ""
  puts "<--------------------------------  #{pos} of #{ARGV[2]} : Started at #{starttime} -------------------------------------->" 
  puts ""
  
  t = Thread.new {
    # Person table and associated fields
    print "# importing person with id #{person["patient_id"]}\n"
      
    begin
      p = dest_con.query("INSERT INTO person SELECT `#{db}`.`patient`.`patient_id`, `#{db}`.`patient`.`gender`, " + 
          "`#{db}`.`patient`.`birthdate`, `#{db}`.`patient`.`birthdate_estimated`, `#{db}`.`patient`.`dead`, " + 
          "`#{db}`.`patient`.`death_date`, `#{db}`.`patient`.`cause_of_death`, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`creator`), `#{db}`.`patient`.`date_created`, "  + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`changed_by`), `#{db}`.`patient`.`date_changed`, `#{db}`.`patient`.`voided`, "  + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`voided_by`), `#{db}`.`patient`.`date_voided`, `#{db}`.`patient`.`void_reason`, " + 
          "(SELECT UUID()) FROM `#{db}`.`patient` " + 
          "WHERE `#{db}`.`patient`.`patient_id` = #{person["patient_id"]} " + 
          " ON DUPLICATE KEY UPDATE person_id = #{person["patient_id"]}")  

    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: "
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO person SELECT `#{db}`.`patient`.`patient_id`, `#{db}`.`patient`.`gender`, " + 
          "`#{db}`.`patient`.`birthdate`, `#{db}`.`patient`.`birthdate_estimated`, `#{db}`.`patient`.`dead`, " + 
          "`#{db}`.`patient`.`death_date`, `#{db}`.`patient`.`cause_of_death`, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`creator`), `#{db}`.`patient`.`date_created`, "  + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`changed_by`), `#{db}`.`patient`.`date_changed`, `#{db}`.`patient`.`voided`, "  + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`voided_by`), `#{db}`.`patient`.`date_voided`, `#{db}`.`patient`.`void_reason`, " + 
          "(SELECT UUID()) FROM `#{db}`.`patient` " + 
          "WHERE `#{db}`.`patient`.`patient_id` = #{person["patient_id"]} " + 
          " ON DUPLICATE KEY UPDATE person_id = #{person["patient_id"]}"
      next
    end
    
    begin
      print "# importing patient with id #{person["patient_id"]}\n"
      
      p = dest_con.query("INSERT INTO patient SELECT `#{db}`.`patient`.`patient_id`, `#{db}`.`patient`.`tribe`, " +
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`creator`), `#{db}`.`patient`.`date_created`, "  + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`changed_by`), `#{db}`.`patient`.`date_changed`, `#{db}`.`patient`.`voided`, "  + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`voided_by`), `#{db}`.`patient`.`date_voided`, `#{db}`.`patient`.`void_reason` " + 
          "FROM `#{db}`.`patient` WHERE `#{db}`.`patient`.`patient_id` = #{person["patient_id"]} " + 
          " ON DUPLICATE KEY UPDATE patient_id = #{person["patient_id"]}")  
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO patient SELECT `#{db}`.`patient`.`patient_id`, `#{db}`.`patient`.`tribe`, " +
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`creator`), `#{db}`.`patient`.`date_created`, "  + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`changed_by`), `#{db}`.`patient`.`date_changed`, `#{db}`.`patient`.`voided`, "  + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`voided_by`), `#{db}`.`patient`.`date_voided`, `#{db}`.`patient`.`void_reason` " + 
          "FROM `#{db}`.`patient` WHERE `#{db}`.`patient`.`patient_id` = #{person["patient_id"]} " + 
          " ON DUPLICATE KEY UPDATE patient_id = #{person["patient_id"]}"
      next
      
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: "
    end
    
    begin
      # Relationships have to be handled later
      print "# importing relationships for patient with id #{person["patient_id"]}\n"
      
      p = dest_con.query("INSERT INTO relationship SELECT `#{db}`.`relationship`.`relationship_id`, #{person["patient_id"]}, " + 
          "(CASE `#{db}`.`relationship`.`relationship` WHEN 1 THEN 7 WHEN 2 OR 4 OR 6 THEN 3 WHEN 7 THEN 11 WHEN 8 THEN 2 " + 
          "WHEN 9 THEN 12 ELSE 13 END), " +
          "(SELECT `#{db}`.`person`.`patient_id` FROM `#{db}`.`person` WHERE `#{db}`.`person`.`person_id` = " + 
          "`#{db}`.`relationship`.`relative_id`), (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` " + 
          "WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`relationship`.`creator`), `#{db}`.`relationship`.`date_created`, " + 
          "`#{db}`.`relationship`.`voided`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`relationship`.`voided_by`), `#{db}`.`relationship`.`date_voided`, " + 
          "`#{db}`.`relationship`.`void_reason`, (SELECT UUID()) FROM `#{db}`.`relationship` WHERE `#{db}`.`relationship`.`person_id` = " + 
          "(SELECT `#{db}`.`person`.`person_id` FROM `#{db}`.`person` WHERE `#{db}`.`person`.`patient_id` = " + 
          "#{person["patient_id"]} LIMIT 1) " + 
          " ON DUPLICATE KEY UPDATE person_a = #{person["patient_id"]}")

    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " 
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO relationship SELECT `#{db}`.`relationship`.`relationship_id`, #{person["patient_id"]}, " + 
          "(CASE `#{db}`.`relationship`.`relationship` WHEN 1 THEN 7 WHEN 2 OR 4 OR 6 THEN 3 WHEN 7 THEN 11 WHEN 8 THEN 2 " + 
          "WHEN 9 THEN 12 ELSE 13 END), " +
          "(SELECT `#{db}`.`person`.`patient_id` FROM `#{db}`.`person` WHERE `#{db}`.`person`.`person_id` = " + 
          "`#{db}`.`relationship`.`relative_id`), (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` " + 
          "WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`relationship`.`creator`), `#{db}`.`relationship`.`date_created`, " + 
          "`#{db}`.`relationship`.`voided`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`relationship`.`voided_by`), `#{db}`.`relationship`.`date_voided`, " + 
          "`#{db}`.`relationship`.`void_reason`, (SELECT UUID()) FROM `#{db}`.`relationship` WHERE `#{db}`.`relationship`.`person_id` = " + 
          "(SELECT `#{db}`.`person`.`person_id` FROM `#{db}`.`person` WHERE `#{db}`.`person`.`patient_id` = " + 
          "#{person["patient_id"]} LIMIT 1) " + 
          " ON DUPLICATE KEY UPDATE person_a = #{person["patient_id"]}"
      next
      
    end
    
    begin
      print "# importing person_address for patient with id #{person["patient_id"]}\n"
      
      p = dest_con.query("INSERT INTO person_address SELECT `#{db}`.`patient_address`.`patient_address_id`, " + 
          "#{person["patient_id"]}, `#{db}`.`patient_address`.`preferred`, `#{db}`.`patient_address`.`address1`, " + 
          "`#{db}`.`patient_address`.`address2`, `city_village`, `#{db}`.`patient_address`.`state_province`, " + 
          "`#{db}`.`patient_address`.`postal_code`, `#{db}`.`patient_address`.`country`, `#{db}`.`patient_address`.`latitude`, " + 
          "`#{db}`.`patient_address`.`longitude`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` " + 
          "WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`patient_address`.`creator`), `#{db}`.`patient_address`.`date_created`, " + 
          "`#{db}`.`patient_address`.`voided`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_address`.`voided_by`), `#{db}`.`patient_address`.`date_voided`, " + 
          "`#{db}`.`patient_address`.`void_reason`, NULL, NULL, NULL, NULL, NULL, (SELECT UUID()) FROM `#{db}`.`patient_address` " + 
          " WHERE `#{db}`.`patient_address`.`patient_id` = #{person["patient_id"]} " + 
          " ON DUPLICATE KEY UPDATE person_address_id = `#{db}`.`patient_address`.`patient_address_id`")
      
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " 
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery" + "INSERT INTO person_address SELECT `#{db}`.`patient_address`.`patient_address_id`, " + 
          "#{person["patient_id"]}, `#{db}`.`patient_address`.`preferred`, `#{db}`.`patient_address`.`address1`, " + 
          "`#{db}`.`patient_address`.`address2`, `city_village`, `#{db}`.`patient_address`.`state_province`, " + 
          "`#{db}`.`patient_address`.`postal_code`, `#{db}`.`patient_address`.`country`, `#{db}`.`patient_address`.`latitude`, " + 
          "`#{db}`.`patient_address`.`longitude`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` " + 
          "WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`patient_address`.`creator`), `#{db}`.`patient_address`.`date_created`, " + 
          "`#{db}`.`patient_address`.`voided`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_address`.`voided_by`), `#{db}`.`patient_address`.`date_voided`, " + 
          "`#{db}`.`patient_address`.`void_reason`, NULL, NULL, NULL, NULL, NULL, (SELECT UUID()) FROM `#{db}`.`patient_address` " + 
          " WHERE `#{db}`.`patient_address`.`patient_id` = #{person["patient_id"]} " + 
          " ON DUPLICATE KEY UPDATE person_address_id = `#{db}`.`patient_address`.`patient_address_id`"
      next
      
    end
    
    begin
      print "# importing person_name for patient with id #{person["patient_id"]}\n"
      
      p = dest_con.query("INSERT INTO person_name SELECT NULL, `#{db}`.`patient_name`.`preferred`, " + 
          "#{person["patient_id"]}, `#{db}`.`patient_name`.`prefix`, `#{db}`.`patient_name`.`given_name`, " + 
          "`#{db}`.`patient_name`.`middle_name`, `#{db}`.`patient_name`.`family_name_prefix`, `#{db}`.`patient_name`.`family_name`, " + 
          "`#{db}`.`patient_name`.`family_name2`, `#{db}`.`patient_name`.`family_name_suffix`, `#{db}`.`patient_name`.`degree`, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` =  " + 
          "`#{db}`.`patient_name`.`creator`), `#{db}`.`patient_name`.`date_created`, `#{db}`.`patient_name`.`voided`, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_name`.`voided_by`), `#{db}`.`patient_name`.`date_voided`, " + 
          "`#{db}`.`patient_name`.`void_reason`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_name`.`changed_by`), `#{db}`.`patient_name`.`date_changed`, (SELECT UUID())" + 
          "FROM `#{db}`.`patient_name` WHERE `#{db}`.`patient_name`.`patient_id` = #{person["patient_id"]}")
      
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " 
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO person_name SELECT NULL, `#{db}`.`patient_name`.`preferred`, " + 
          "#{person["patient_id"]}, `#{db}`.`patient_name`.`prefix`, `#{db}`.`patient_name`.`given_name`, " + 
          "`#{db}`.`patient_name`.`middle_name`, `#{db}`.`patient_name`.`family_name_prefix`, `#{db}`.`patient_name`.`family_name`, " + 
          "`#{db}`.`patient_name`.`family_name2`, `#{db}`.`patient_name`.`family_name_suffix`, `#{db}`.`patient_name`.`degree`, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` =  " + 
          "`#{db}`.`patient_name`.`creator`), `#{db}`.`patient_name`.`date_created`, `#{db}`.`patient_name`.`voided`, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_name`.`voided_by`), `#{db}`.`patient_name`.`date_voided`, " + 
          "`#{db}`.`patient_name`.`void_reason`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_name`.`changed_by`), `#{db}`.`patient_name`.`date_changed`, (SELECT UUID())" + 
          "FROM `#{db}`.`patient_name` WHERE `#{db}`.`patient_name`.`patient_id` = #{person["patient_id"]}"
      next
      
    end
    
    begin
      print "# importing person_identifiers for patient with id #{person["patient_id"]}\n"
      
      p = dest_con.query("INSERT INTO patient_identifier SELECT NULL, #{person["patient_id"]}, `#{db}`.`patient_identifier`.`identifier`, " + 
          "COALESCE((SELECT patient_identifier_type_id FROM patient_identifier_type WHERE name = " + 
          "(SELECT name FROM `#{db}`.`patient_identifier_type` WHERE `#{db}`.`patient_identifier_type`.`patient_identifier_type_id` = " + 
          "`#{db}`.`patient_identifier`.`identifier_type`)), NULL), `#{db}`.`patient_identifier`.`preferred`, " + 
          "`#{db}`.`patient_identifier`.`location_id`, (SELECT `users_mapping`.`bart2_user_id` FROM " + 
          "`users_mapping` WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`patient_identifier`.`creator`), " + 
          "`#{db}`.`patient_identifier`.`date_created`, `#{db}`.`patient_identifier`.`voided`, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_identifier`.`voided_by`), `#{db}`.`patient_identifier`.`date_voided`, " + 
          "`#{db}`.`patient_identifier`.`void_reason`, (SELECT UUID())" + 
          "FROM `#{db}`.`patient_identifier` WHERE `#{db}`.`patient_identifier`.`patient_id` = #{person["patient_id"]} AND NOT " + 
          "`#{}`.`patient_identifier`.`identifier_type` IN (SELECT `#{db}`.`patient_identifier_type`.`patient_identifier_type_id` FROM " + 
          " `#{db}`.`patient_identifier_type` WHERE name IN ('Occupation', 'Cell phone number', 'Physical address', " + 
          "'Traditional authority', 'Home phone number', 'Office phone number'))")
    
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " 
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO patient_identifier SELECT NULL, #{person["patient_id"]}, `#{db}`.`patient_identifier`.`identifier`, " + 
          "COALESCE((SELECT patient_identifier_type_id FROM patient_identifier_type WHERE name = " + 
          "(SELECT name FROM `#{db}`.`patient_identifier_type` WHERE `#{db}`.`patient_identifier_type`.`patient_identifier_type_id` = " + 
          "`#{db}`.`patient_identifier`.`identifier_type`)), NULL), `#{db}`.`patient_identifier`.`preferred`, " + 
          "`#{db}`.`patient_identifier`.`location_id`, (SELECT `users_mapping`.`bart2_user_id` FROM " + 
          "`users_mapping` WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`patient_identifier`.`creator`), " + 
          "`#{db}`.`patient_identifier`.`date_created`, `#{db}`.`patient_identifier`.`voided`, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_identifier`.`voided_by`), `#{db}`.`patient_identifier`.`date_voided`, " + 
          "`#{db}`.`patient_identifier`.`void_reason`, (SELECT UUID())" + 
          "FROM `#{db}`.`patient_identifier` WHERE `#{db}`.`patient_identifier`.`patient_id` = #{person["patient_id"]} AND NOT " + 
          "`#{}`.`patient_identifier`.`identifier_type` IN (SELECT `#{db}`.`patient_identifier_type`.`patient_identifier_type_id` FROM " + 
          " `#{db}`.`patient_identifier_type` WHERE name IN ('Occupation', 'Cell phone number', 'Physical address', " + 
          "'Traditional authority', 'Home phone number', 'Office phone number'))"
      next
      
    end
    
    begin
      print "# importing person_attributes for patient with id #{person["patient_id"]}\n"
      
      p = dest_con.query("INSERT INTO person_attribute SELECT NULL, #{person["patient_id"]}, `#{db}`.`patient_identifier`.`identifier`, " + 
          "COALESCE((SELECT person_attribute_type_id FROM person_attribute_type WHERE name = " + 
          "(SELECT name FROM `#{db}`.`patient_identifier_type` WHERE `#{db}`.`patient_identifier_type`.`patient_identifier_type_id` = " + 
          "`#{db}`.`patient_identifier`.`identifier_type`)), NULL), `#{db}`.`patient_identifier`.`preferred`, " + 
          "`#{db}`.`patient_identifier`.`location_id`, (SELECT `users_mapping`.`bart2_user_id` FROM " + 
          "`users_mapping` WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`patient_identifier`.`creator`), " + 
          "`#{db}`.`patient_identifier`.`date_created`, `#{db}`.`patient_identifier`.`voided`, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_identifier`.`voided_by`), `#{db}`.`patient_identifier`.`date_voided`, " + 
          "`#{db}`.`patient_identifier`.`void_reason`, (SELECT UUID())" + 
          "FROM `#{db}`.`patient_identifier` WHERE `#{db}`.`patient_identifier`.`patient_id` = #{person["patient_id"]} AND " + 
          "`#{}`.`patient_identifier`.`identifier_type` IN (SELECT `#{db}`.`patient_identifier_type`.`patient_identifier_type_id` FROM " + 
          " `#{db}`.`patient_identifier_type` WHERE name IN ('Occupation', 'Cell phone number', 'Physical address', " + 
          "'Traditional authority', 'Home phone number', 'Office phone number'))")
    
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " 
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO person_attribute SELECT NULL, #{person["patient_id"]}, `#{db}`.`patient_identifier`.`identifier`, " + 
          "COALESCE((SELECT person_attribute_type_id FROM person_attribute_type WHERE name = " + 
          "(SELECT name FROM `#{db}`.`patient_identifier_type` WHERE `#{db}`.`patient_identifier_type`.`patient_identifier_type_id` = " + 
          "`#{db}`.`patient_identifier`.`identifier_type`)), NULL), `#{db}`.`patient_identifier`.`preferred`, " + 
          "`#{db}`.`patient_identifier`.`location_id`, (SELECT `users_mapping`.`bart2_user_id` FROM " + 
          "`users_mapping` WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`patient_identifier`.`creator`), " + 
          "`#{db}`.`patient_identifier`.`date_created`, `#{db}`.`patient_identifier`.`voided`, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_identifier`.`voided_by`), `#{db}`.`patient_identifier`.`date_voided`, " + 
          "`#{db}`.`patient_identifier`.`void_reason`, (SELECT UUID())" + 
          "FROM `#{db}`.`patient_identifier` WHERE `#{db}`.`patient_identifier`.`patient_id` = #{person["patient_id"]} AND " + 
          "`#{}`.`patient_identifier`.`identifier_type` IN (SELECT `#{db}`.`patient_identifier_type`.`patient_identifier_type_id` FROM " + 
          " `#{db}`.`patient_identifier_type` WHERE name IN ('Occupation', 'Cell phone number', 'Physical address', " + 
          "'Traditional authority', 'Home phone number', 'Office phone number'))"
      next
      
    end
    
    begin
      print "# importing person_programs for patient with id #{person["patient_id"]}\n"
      
      p = dest_con.query("INSERT INTO patient_program SELECT `#{db}`.`patient_program`.`patient_program_id`, " + 
          "#{person["patient_id"]}, `#{db}`.`patient_program`.`program_id`, `#{db}`.`patient_program`.`date_enrolled`, " + 
          "`#{db}`.`patient_program`.`date_completed`, (SELECT `users_mapping`.`bart2_user_id` FROM " + 
          "`users_mapping` WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`patient_program`.`creator`), " + 
          "`#{db}`.`patient_program`.`date_created`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_program`.`changed_by`), `#{db}`.`patient_program`.`date_changed`, " + 
          "`#{db}`.`patient_program`.`voided`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_program`.`voided_by`), `#{db}`.`patient_program`.`date_voided`, " + 
          "`#{db}`.`patient_program`.`void_reason`, (SELECT UUID()), NULL " + 
          " FROM `#{db}`.`patient_program` WHERE `#{db}`.`patient_program`.`patient_id` = #{person["patient_id"]} " + 
          " ON DUPLICATE KEY UPDATE patient_program_id = `#{db}`.`patient_program`.`patient_program_id`")    
      
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " 
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO patient_program SELECT `#{db}`.`patient_program`.`patient_program_id`, " + 
          "#{person["patient_id"]}, `#{db}`.`patient_program`.`program_id`, `#{db}`.`patient_program`.`date_enrolled`, " + 
          "`#{db}`.`patient_program`.`date_completed`, (SELECT `users_mapping`.`bart2_user_id` FROM " + 
          "`users_mapping` WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`patient_program`.`creator`), " + 
          "`#{db}`.`patient_program`.`date_created`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_program`.`changed_by`), `#{db}`.`patient_program`.`date_changed`, " + 
          "`#{db}`.`patient_program`.`voided`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_program`.`voided_by`), `#{db}`.`patient_program`.`date_voided`, " + 
          "`#{db}`.`patient_program`.`void_reason`, (SELECT UUID()), NULL " + 
          " FROM `#{db}`.`patient_program` WHERE `#{db}`.`patient_program`.`patient_id` = #{person["patient_id"]} " + 
          " ON DUPLICATE KEY UPDATE patient_program_id = `#{db}`.`patient_program`.`patient_program_id`"
      next
      
    end
    
    begin
      print "# importing patient_states for patient with id #{person["patient_id"]}\n"
      
      p = dest_con.query("INSERT INTO patient_state SELECT NULL, (SELECT patient_program_id FROM patient_program WHERE " + 
          " patient_id = #{person["patient_id"]} ORDER BY date_created DESC LIMIT 1), " + 
          "(SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = " + 
          "(SELECT concept_id FROM concept_name WHERE name = 'On antiretrovirals' LIMIT 1) " + 
          "AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = " + 
          "(SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = " + 
          "'HIV PROGRAM' LIMIT 1)))), `#{db}`.`orders`.`date_created`, NULL, " + 
          "(SELECT creator FROM encounter WHERE encounter_id = `#{db}`.`orders`.`encounter_id`), " + 
          "`#{db}`.`orders`.`date_created`, NULL, NULL, NULL, NULL, NULL, NULL, (SELECT UUID()) " + 
          " FROM `#{db}`.`orders` WHERE " + 
          "`#{db}`.`orders`.`encounter_id` = (SELECT `#{db}`.`encounter`.`encounter_id` FROM `#{db}`.`encounter` " + 
          " WHERE `#{db}`.`encounter`.`encounter_id` = `#{db}`.`encounter`.`encounter_id` AND "  + 
          "`#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} AND `#{db}`.`encounter`.`encounter_type` = " + 
          "(SELECT encounter_type_id FROM `#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`name` = 'Give Drugs') " + 
          " AND `#{db}`.`encounter`.`patient_id` IN (SELECT DISTINCT `#{db}`.`patient_program`.`patient_id` " + 
          " FROM `#{db}`.`patient_program`) LIMIT 1) " + 
          "AND COALESCE((`#{db}`.regimen_category(#{person["patient_id"]}, " + 
          "(SELECT `#{db}`.`drug_order`.`drug_inventory_id` FROM `#{db}`.`drug_order` WHERE `#{db}`.`drug_order`.`order_id` = " + 
          "`#{db}`.`orders`.`order_id` LIMIT 1), DATE(`#{db}`.`orders`.`date_created`))),'') != ''")
      
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " + "INSERT INTO patient_state SELECT NULL, (SELECT patient_program_id FROM patient_program WHERE " + 
        " patient_id = #{person["patient_id"]} ORDER BY date_created DESC LIMIT 1), " + 
        "(SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = " + 
        "(SELECT concept_id FROM concept_name WHERE name = 'On antiretrovirals' LIMIT 1) " + 
        "AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = " + 
        "(SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = " + 
        "'HIV PROGRAM' LIMIT 1)))), `#{db}`.`orders`.`date_created`, NULL, " + 
        "(SELECT creator FROM encounter WHERE encounter_id = `#{db}`.`orders`.`encounter_id`), " + 
        "`#{db}`.`orders`.`date_created`, NULL, NULL, NULL, NULL, NULL, NULL, (SELECT UUID()) " + 
        " FROM `#{db}`.`orders` WHERE " + 
        "`#{db}`.`orders`.`encounter_id` = (SELECT `#{db}`.`encounter`.`encounter_id` FROM `#{db}`.`encounter` " + 
        " WHERE `#{db}`.`encounter`.`encounter_id` = `#{db}`.`encounter`.`encounter_id` AND "  + 
        "`#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} AND `#{db}`.`encounter`.`encounter_type` = " + 
        "(SELECT encounter_type_id FROM `#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`name` = 'Give Drugs') " + 
        " AND `#{db}`.`encounter`.`patient_id` IN (SELECT DISTINCT `#{db}`.`patient_program`.`patient_id` " + 
        " FROM `#{db}`.`patient_program`) LIMIT 1) " + 
        "AND COALESCE((`#{db}`.regimen_category(#{person["patient_id"]}, " + 
        "(SELECT `#{db}`.`drug_order`.`drug_inventory_id` FROM `#{db}`.`drug_order` WHERE `#{db}`.`drug_order`.`order_id` = " + 
        "`#{db}`.`orders`.`order_id` LIMIT 1), DATE(`#{db}`.`orders`.`date_created`))),'') != ''" 
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO patient_state SELECT NULL, (SELECT patient_program_id FROM patient_program WHERE " + 
          " patient_id = #{person["patient_id"]} ORDER BY date_created DESC LIMIT 1), " + 
          "(SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = " + 
          "(SELECT concept_id FROM concept_name WHERE name = 'On antiretrovirals' LIMIT 1) " + 
          "AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = " + 
          "(SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = " + 
          "'HIV PROGRAM' LIMIT 1)))), `#{db}`.`orders`.`date_created`, NULL, " + 
          "(SELECT creator FROM encounter WHERE encounter_id = `#{db}`.`orders`.`encounter_id`), " + 
          "`#{db}`.`orders`.`date_created`, NULL, NULL, NULL, NULL, NULL, NULL, (SELECT UUID()) " + 
          " FROM `#{db}`.`orders` WHERE " + 
          "`#{db}`.`orders`.`encounter_id` = (SELECT `#{db}`.`encounter`.`encounter_id` FROM `#{db}`.`encounter` " + 
          " WHERE `#{db}`.`encounter`.`encounter_id` = `#{db}`.`encounter`.`encounter_id` AND "  + 
          "`#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} AND `#{db}`.`encounter`.`encounter_type` = " + 
          "(SELECT encounter_type_id FROM `#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`name` = 'Give Drugs') " + 
          " AND `#{db}`.`encounter`.`patient_id` IN (SELECT DISTINCT `#{db}`.`patient_program`.`patient_id` " + 
          " FROM `#{db}`.`patient_program`) LIMIT 1) " + 
          "AND COALESCE((`#{db}`.regimen_category(#{person["patient_id"]}, " + 
          "(SELECT `#{db}`.`drug_order`.`drug_inventory_id` FROM `#{db}`.`drug_order` WHERE `#{db}`.`drug_order`.`order_id` = " + 
          "`#{db}`.`orders`.`order_id` LIMIT 1), DATE(`#{db}`.`orders`.`date_created`))),'') != ''"
      next
      
    end
    
    begin
      print "# importing encounters for patient with id #{person["patient_id"]}\n"
      
      p = dest_con.query("INSERT INTO encounter SELECT `#{db}`.`encounter`.`encounter_id`, " + 
          "COALESCE((SELECT encounter_type_id FROM encounter_type WHERE name = " + 
          "(SELECT CASE `#{db}`.`encounter_type`.`name` WHEN 'HIV FIRST VISIT' THEN 'HIV CLINIC REGISTRATION' " + 
          "WHEN 'ART VISIT' THEN 'HIV CLINIC CONSULTATION'" +
          "WHEN 'GIVE DRUGS' THEN 'DISPENSING'" +          
          "WHEN 'DATE OF ART INITIATION' THEN 'ART ENROLLMENT'" +
          "WHEN 'HEIGHT/WEIGHT' THEN 'VITALS'" +
          "WHEN 'GENERAL RECEPTION' OR 'BARCODE SCAN' THEN 'REGISTRATION'" + 
          "WHEN 'TB RECEPTION' THEN 'TB CLINIC VISIT'" + 
          "WHEN 'REFFERED' THEN 'IS PATIENT REFERRED?'" +
          "END FROM " + 
          "`#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`encounter_type_id` = `#{db}`.`encounter`.`encounter_type`)),NULL)" +
          ", #{person["patient_id"]}, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`encounter`.`provider_id`), `#{db}`.`encounter`.`location_id`, " + 
          "`#{db}`.`encounter`.`form_id`, `#{db}`.`encounter`.`encounter_datetime`, " +
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`encounter`.`creator`), `#{db}`.`encounter`.`date_created`, " + 
          "NULL, NULL, NULL, NULL, (SELECT UUID()), NULL, NULL " + 
          "FROM `#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} " + 
          " ON DUPLICATE KEY UPDATE encounter_id = `#{db}`.`encounter`.`encounter_id`")
      
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " 
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO encounter SELECT `#{db}`.`encounter`.`encounter_id`, " + 
          "COALESCE((SELECT encounter_type_id FROM encounter_type WHERE name = " + 
          "(SELECT CASE `#{db}`.`encounter_type`.`name` WHEN 'HIV FIRST VISIT' THEN 'HIV CLINIC REGISTRATION' " + 
          "WHEN 'ART VISIT' THEN 'HIV CLINIC CONSULTATION'" +
          "WHEN 'GIVE DRUGS' THEN 'DISPENSING'" +          
          "WHEN 'DATE OF ART INITIATION' THEN 'ART ENROLLMENT'" +
          "WHEN 'HEIGHT/WEIGHT' THEN 'VITALS'" +
          "WHEN 'GENERAL RECEPTION' OR 'BARCODE SCAN' THEN 'REGISTRATION'" + 
          "WHEN 'TB RECEPTION' THEN 'TB CLINIC VISIT'" + 
          "WHEN 'REFFERED' THEN 'IS PATIENT REFERRED?'" +
          "END FROM " + 
          "`#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`encounter_type_id` = `#{db}`.`encounter`.`encounter_type`)),NULL)" +
          ", #{person["patient_id"]}, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`encounter`.`provider_id`), `#{db}`.`encounter`.`location_id`, " + 
          "`#{db}`.`encounter`.`form_id`, `#{db}`.`encounter`.`encounter_datetime`, " +
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`encounter`.`creator`), `#{db}`.`encounter`.`date_created`, " + 
          "NULL, NULL, NULL, NULL, (SELECT UUID()), NULL, NULL " + 
          "FROM `#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} " + 
          " ON DUPLICATE KEY UPDATE encounter_id = `#{db}`.`encounter`.`encounter_id`"
      next
      
    end
    
    begin      
      print "# importing obs for patient with id #{person["patient_id"]}\n"
      
      pp = dest_con.query("INSERT INTO obs SELECT `#{db}`.`obs`.`obs_id` obs_id, #{person["patient_id"]} patient_id, " + 
          "(CASE WHEN `#{db}`.`obs`.`concept_id` IN (SELECT old_concept_id FROM tmp_concepts_stack t WHERE old_concept_id = `#{db}`.`obs`.`concept_id` AND " + 
          " old_concept_name IN ('Peripheral neuropathy',  'Leg pain / numbness', 'Hepatitis',  'Jaundice', 'Skin rash', 'Lipodystrophy', " + 
          " 'Lactic acidosis', 'Anaemia', 'Other symptom', 'Other side effect')) AND `#{db}`.`obs`.`value_coded` IN " + 
          "(SELECT old_concept_id FROM tmp_concepts_stack t where old_concept_name IN ('Yes', 'Yes drug induced', " + 
          " 'Yes not drug induced', 'Yes unknown cause')) THEN (SELECT concept_id FROM concept_name " + 
          "WHERE name = 'DRUG INDUCED') ELSE " + 
          "(SELECT new_concept_id FROM tmp_concepts_stack WHERE old_concept_id = `#{db}`.`obs`.`concept_id` LIMIT 1) END) concept_id, " + 
          " `#{db}`.`obs`.`encounter_id` encounter_id, `#{db}`.`obs`.`order_id` order_id, " + 
          "`#{db}`.`obs`.`obs_datetime` obs_datetime, `#{db}`.`obs`.`location_id` location_id, " + 
          "`#{db}`.`obs`.`obs_group_id` obs_group_id, `#{db}`.`obs`.`accession_number` accession_number, " + 
          "`#{db}`.`obs`.`value_group_id` value_group_id, `#{db}`.`obs`.`value_boolean` value_boolean, " + 
          "(CASE WHEN `#{db}`.`obs`.`concept_id` IN (SELECT old_concept_id FROM tmp_concepts_stack t WHERE old_concept_id = `#{db}`.`obs`.`concept_id` AND " + 
          " old_concept_name IN ('Peripheral neuropathy',  'Leg pain / numbness', 'Hepatitis',  'Jaundice', 'Skin rash', 'Lipodystrophy', " + 
          " 'Lactic acidosis', 'Anaemia', 'Other symptom', 'Other side effect')) AND `#{db}`.`obs`.`value_coded` IN " + 
          "(SELECT old_concept_id FROM tmp_concepts_stack t where old_concept_name IN ('Yes', 'Yes drug induced', " + 
          " 'Yes not drug induced', 'Yes unknown cause')) THEN `#{db}`.`obs`.`concept_id` ELSE " + 
          "(SELECT new_concept_id FROM tmp_concepts_stack WHERE old_concept_id = `#{db}`.`obs`.`value_coded` LIMIT 1) END) value_coded, " + 
          "NULL, `#{db}`.`obs`.`value_drug` value_drug, " + 
          "`#{db}`.`obs`.`value_datetime` value_datetime, `#{db}`.`obs`.`value_numeric` value_numeric, " + 
          "`#{db}`.`obs`.`value_modifier` value_modifier, `#{db}`.`obs`.`value_text` value_text, " + 
          "`#{db}`.`obs`.`date_started` date_started, `#{db}`.`obs`.`date_stopped` date_stopped, `#{db}`.`obs`.`comments` comments, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`obs`.`creator`) creator, `#{db}`.`obs`.`date_created` date_created, `#{db}`.`obs`.`voided` voided, " + 
          " (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`obs`.`voided_by`) voided_by, `#{db}`.`obs`.`date_voided` date_voided, `#{db}`.`obs`.`void_reason` void_reason, " + 
          "NULL, (SELECT UUID()) uuid " + 
          "FROM `#{db}`.`obs` WHERE `#{db}`.`obs`.`patient_id` = #{person["patient_id"]} ")
      
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " + "INSERT INTO obs SELECT `#{db}`.`obs`.`obs_id` obs_id, #{person["patient_id"]} patient_id, " + 
        "(CASE WHEN `#{db}`.`obs`.`concept_id` IN (SELECT old_concept_id FROM tmp_concepts_stack t WHERE old_concept_id = `#{db}`.`obs`.`concept_id` AND " + 
        " old_concept_name IN ('Peripheral neuropathy',  'Leg pain / numbness', 'Hepatitis',  'Jaundice', 'Skin rash', 'Lipodystrophy', " + 
        " 'Lactic acidosis', 'Anaemia', 'Other symptom', 'Other side effect')) THEN (SELECT concept_id FROM concept_name " + 
        "WHERE name = 'DRUG INDUCED') ELSE " + 
        "(SELECT new_concept_id FROM tmp_concepts_stack WHERE old_concept_id = `#{db}`.`obs`.`concept_id` LIMIT 1) END) concept_id, " + 
        " `#{db}`.`obs`.`encounter_id` encounter_id, `#{db}`.`obs`.`order_id` order_id, " + 
        "`#{db}`.`obs`.`obs_datetime` obs_datetime, `#{db}`.`obs`.`location_id` location_id, " + 
        "`#{db}`.`obs`.`obs_group_id` obs_group_id, `#{db}`.`obs`.`accession_number` accession_number, " + 
        "`#{db}`.`obs`.`value_group_id` value_group_id, `#{db}`.`obs`.`value_boolean` value_boolean, " + 
        "(CASE WHEN `#{db}`.`obs`.`concept_id` IN (SELECT old_concept_id FROM tmp_concepts_stack t WHERE old_concept_id = `#{db}`.`obs`.`concept_id` AND " + 
        " old_concept_name IN ('Peripheral neuropathy',  'Leg pain / numbness', 'Hepatitis',  'Jaundice', 'Skin rash', 'Lipodystrophy', " + 
        " 'Lactic acidosis', 'Anaemia', 'Other symptom', 'Other side effect')) THEN `#{db}`.`obs`.`concept_id` ELSE " + 
        "(SELECT new_concept_id FROM tmp_concepts_stack WHERE old_concept_id = `#{db}`.`obs`.`value_coded` LIMIT 1) END) value_coded, " + 
        "NULL, `#{db}`.`obs`.`value_drug` value_drug, " + 
        "`#{db}`.`obs`.`value_datetime` value_datetime, `#{db}`.`obs`.`value_numeric` value_numeric, " + 
        "`#{db}`.`obs`.`value_modifier` value_modifier, `#{db}`.`obs`.`value_text` value_text, " + 
        "`#{db}`.`obs`.`date_started` date_started, `#{db}`.`obs`.`date_stopped` date_stopped, `#{db}`.`obs`.`comments` comments, " + 
        "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
        "`#{db}`.`obs`.`creator`) creator, `#{db}`.`obs`.`date_created` date_created, `#{db}`.`obs`.`voided` voided, " + 
        " (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
        "`#{db}`.`obs`.`voided_by`) voided_by, `#{db}`.`obs`.`date_voided` date_voided, `#{db}`.`obs`.`void_reason` void_reason, " + 
        "NULL, (SELECT UUID()) uuid " + 
        "FROM `#{db}`.`obs` WHERE `#{db}`.`obs`.`patient_id` = #{person["patient_id"]} " 
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO obs SELECT `#{db}`.`obs`.`obs_id` obs_id, #{person["patient_id"]} patient_id, " + 
          "(CASE WHEN `#{db}`.`obs`.`concept_id` IN (SELECT old_concept_id FROM tmp_concepts_stack t WHERE old_concept_id = `#{db}`.`obs`.`concept_id` AND " + 
          " old_concept_name IN ('Peripheral neuropathy',  'Leg pain / numbness', 'Hepatitis',  'Jaundice', 'Skin rash', 'Lipodystrophy', " + 
          " 'Lactic acidosis', 'Anaemia', 'Other symptom', 'Other side effect')) AND `#{db}`.`obs`.`value_coded` IN " + 
          "(SELECT old_concept_id FROM tmp_concepts_stack t where old_concept_name IN ('Yes', 'Yes drug induced', " + 
          " 'Yes not drug induced', 'Yes unknown cause')) THEN (SELECT concept_id FROM concept_name " + 
          "WHERE name = 'DRUG INDUCED') ELSE " + 
          "(SELECT new_concept_id FROM tmp_concepts_stack WHERE old_concept_id = `#{db}`.`obs`.`concept_id` LIMIT 1) END) concept_id, " + 
          " `#{db}`.`obs`.`encounter_id` encounter_id, `#{db}`.`obs`.`order_id` order_id, " + 
          "`#{db}`.`obs`.`obs_datetime` obs_datetime, `#{db}`.`obs`.`location_id` location_id, " + 
          "`#{db}`.`obs`.`obs_group_id` obs_group_id, `#{db}`.`obs`.`accession_number` accession_number, " + 
          "`#{db}`.`obs`.`value_group_id` value_group_id, `#{db}`.`obs`.`value_boolean` value_boolean, " + 
          "(CASE WHEN `#{db}`.`obs`.`concept_id` IN (SELECT old_concept_id FROM tmp_concepts_stack t WHERE old_concept_id = `#{db}`.`obs`.`concept_id` AND " + 
          " old_concept_name IN ('Peripheral neuropathy',  'Leg pain / numbness', 'Hepatitis',  'Jaundice', 'Skin rash', 'Lipodystrophy', " + 
          " 'Lactic acidosis', 'Anaemia', 'Other symptom', 'Other side effect')) AND `#{db}`.`obs`.`value_coded` IN " + 
          "(SELECT old_concept_id FROM tmp_concepts_stack t where old_concept_name IN ('Yes', 'Yes drug induced', " + 
          " 'Yes not drug induced', 'Yes unknown cause')) THEN `#{db}`.`obs`.`concept_id` ELSE " + 
          "(SELECT new_concept_id FROM tmp_concepts_stack WHERE old_concept_id = `#{db}`.`obs`.`value_coded` LIMIT 1) END) value_coded, " + 
          "NULL, `#{db}`.`obs`.`value_drug` value_drug, " + 
          "`#{db}`.`obs`.`value_datetime` value_datetime, `#{db}`.`obs`.`value_numeric` value_numeric, " + 
          "`#{db}`.`obs`.`value_modifier` value_modifier, `#{db}`.`obs`.`value_text` value_text, " + 
          "`#{db}`.`obs`.`date_started` date_started, `#{db}`.`obs`.`date_stopped` date_stopped, `#{db}`.`obs`.`comments` comments, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`obs`.`creator`) creator, `#{db}`.`obs`.`date_created` date_created, `#{db}`.`obs`.`voided` voided, " + 
          " (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`obs`.`voided_by`) voided_by, `#{db}`.`obs`.`date_voided` date_voided, `#{db}`.`obs`.`void_reason` void_reason, " + 
          "NULL, (SELECT UUID()) uuid " + 
          "FROM `#{db}`.`obs` WHERE `#{db}`.`obs`.`patient_id` = #{person["patient_id"]} "
      next
      
    end

    if ARGV[3] == "1" || ARGV[3] == "true"
      begin
        print "# insert into obs reason for art eligibity for patient with id #{person["patient_id"]}\n"
            
        w = dest_con.query("SELECT `#{db}`.who_stage(#{person["patient_id"]}, DATE(NOW())) stage")
      
        row = w.fetch_hash
      
        puts "WHO Stage " + row["stage"].to_s
      
        if row["stage"].to_i > 1
          p = dest_con.query("SELECT `#{db}`.reason_for_art_eligibility(#{person["patient_id"]}) reason")
      
          row = p.fetch_hash
      
          if !row["reason"].nil?
            puts "Reason " + row["reason"]
        
            p = dest_con.query("INSERT INTO obs SELECT NULL, #{person["patient_id"]}, " + 
                "(SELECT concept_id FROM concept_name WHERE name = 'Reason for ART eligibility' LIMIT 1), " + 
                " `#{db}`.`encounter`.`encounter_id`, NULL, `#{db}`.`encounter`.`encounter_datetime`, " + 
                "`#{db}`.`encounter`.`location_id`, NULL, NULL, NULL, NULL, " + 
                "(SELECT new_concept_id FROM tmp_concepts_stack WHERE old_concept_id = " + 
                "(#{row["reason"].to_i}) LIMIT 1), " + 
                "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, " + 
                "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
                "`#{db}`.`encounter`.`creator`), `#{db}`.`encounter`.`date_created`, NULL, " + 
                " NULL, NULL, NULL, NULL, (SELECT UUID()) " + 
                "FROM `#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} AND " + 
                "`#{db}`.`encounter`.`encounter_type` = (SELECT `#{db}`.`encounter_type`.`encounter_type_id` FROM " + 
                "`#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`name` = 'HIV Staging' LIMIT 1) LIMIT 1")
          end
        end
      rescue Mysql::Error => e
        puts "?? Error #{e.errno}: #{e.error}"
      
        puts ":: Query: " + "INSERT INTO obs SELECT NULL, #{person["patient_id"]}, " + 
          "(SELECT concept_id FROM concept_name WHERE name = 'Reason for ART eligibility' LIMIT 1), " + 
          " `#{db}`.`encounter`.`encounter_id`, NULL, `#{db}`.`encounter`.`encounter_datetime`, " + 
          "`#{db}`.`encounter`.`location_id`, NULL, NULL, NULL, NULL, " + 
          "(SELECT new_concept_id FROM tmp_concepts_stack WHERE old_concept_id = " + 
          "(#{row["reason"].to_i}) LIMIT 1), " + 
          "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`encounter`.`creator`), `#{db}`.`encounter`.`date_created`, NULL, " + 
          " NULL, NULL, NULL, NULL, (SELECT UUID()) " + 
          "FROM `#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} AND " + 
          "`#{db}`.`encounter`.`encounter_type` = (SELECT `#{db}`.`encounter_type`.`encounter_type_id` FROM " + 
          "`#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`name` = 'HIV Staging' LIMIT 1) LIMIT 1" 
      
        log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO obs SELECT NULL, #{person["patient_id"]}, " + 
                "(SELECT concept_id FROM concept_name WHERE name = 'Reason for ART eligibility' LIMIT 1), " + 
                " `#{db}`.`encounter`.`encounter_id`, NULL, `#{db}`.`encounter`.`encounter_datetime`, " + 
                "`#{db}`.`encounter`.`location_id`, NULL, NULL, NULL, NULL, " + 
                "(SELECT new_concept_id FROM tmp_concepts_stack WHERE old_concept_id = " + 
                "(#{row["reason"].to_i}) LIMIT 1), " + 
                "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, " + 
                "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
                "`#{db}`.`encounter`.`creator`), `#{db}`.`encounter`.`date_created`, NULL, " + 
                " NULL, NULL, NULL, NULL, (SELECT UUID()) " + 
                "FROM `#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} AND " + 
                "`#{db}`.`encounter`.`encounter_type` = (SELECT `#{db}`.`encounter_type`.`encounter_type_id` FROM " + 
                "`#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`name` = 'HIV Staging' LIMIT 1) LIMIT 1"
        next
      
      end
    end
    
    begin
      print "# importing special obs cases for patient with id #{person["patient_id"]}\n"
      
      p = dest_con.query("INSERT INTO obs SELECT NULL, #{person["patient_id"]}, (SELECT concept_id FROM concept_name WHERE name = " + 
          "'Regimen category' LIMIT 1), `#{db}`.`orders`.`encounter_id`, NULL, " + 
          "`#{db}`.`orders`.`date_created`, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, " + 
          "NULL, NULL, NULL, (`#{db}`.regimen_category(#{person["patient_id"]}, " + 
          "(SELECT `#{db}`.`drug`.`concept_id` FROM `#{db}`.`drug` LEFT OUTER JOIN `#{db}`.`drug_order` ON `#{db}`.`drug`.`drug_id` = " + 
          " `#{db}`.`drug_order`.`drug_inventory_id` WHERE `#{db}`.`drug_order`.`order_id` = " + 
          "`#{db}`.`orders`.`order_id` LIMIT 1), DATE(`#{db}`.`orders`.`date_created`))), NULL, NULL, NULL, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "(SELECT creator FROM encounter WHERE encounter_id = " + 
          "`#{db}`.`orders`.`encounter_id`)), `#{db}`.`orders`.`date_created`, NULL, NULL, " + 
          "NULL, NULL, NULL, (SELECT UUID()) FROM `#{db}`.`orders` WHERE " + 
          "`#{db}`.`orders`.`encounter_id` = (SELECT `#{db}`.`encounter`.`encounter_id` FROM `#{db}`.`encounter` " + 
          " WHERE `#{db}`.`encounter`.`encounter_id` = `#{db}`.`encounter`.`encounter_id` AND "  + 
          "`#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} AND `#{db}`.`encounter`.`encounter_type` = " + 
          "(SELECT encounter_type_id FROM `#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`name` = 'Give Drugs') " + 
          " AND `#{db}`.`encounter`.`patient_id` IN (SELECT DISTINCT `#{db}`.`patient_program`.`patient_id` " + 
          " FROM `#{db}`.`patient_program`) LIMIT 1) " + 
          "AND COALESCE((`#{db}`.regimen_category(#{person["patient_id"]}, " + 
          "(SELECT `#{db}`.`drug_order`.`drug_inventory_id` FROM `#{db}`.`drug_order` WHERE `#{db}`.`drug_order`.`order_id` = " + 
          "`#{db}`.`orders`.`order_id` LIMIT 1), DATE(`#{db}`.`orders`.`date_created`))),'') != ''")
      
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " + "INSERT INTO obs SELECT NULL, #{person["patient_id"]}, (SELECT concept_id FROM concept_name WHERE name = " + 
        "'Regimen category' LIMIT 1), `#{db}`.`orders`.`encounter_id`, NULL, " + 
        "`#{db}`.`orders`.`date_created`, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, " + 
        "NULL, NULL, NULL, (`#{db}`.regimen_category(#{person["patient_id"]}, " + 
        "(SELECT `#{db}`.`drug`.`concept_id` FROM `#{db}`.`drug` LEFT OUTER JOIN `#{db}`.`drug_order` ON `#{db}`.`drug`.`drug_id` = " + 
        " `#{db}`.`drug_order`.`drug_inventory_id` WHERE `#{db}`.`drug_order`.`order_id` = " + 
        "`#{db}`.`orders`.`order_id` LIMIT 1), DATE(`#{db}`.`orders`.`date_created`))), NULL, NULL, NULL, " + 
        "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
        "(SELECT creator FROM encounter WHERE encounter_id = " + 
        "`#{db}`.`orders`.`encounter_id`)), `#{db}`.`orders`.`date_created`, NULL, NULL, " + 
        "NULL, NULL, NULL, (SELECT UUID()) FROM `#{db}`.`orders` WHERE " + 
        "`#{db}`.`orders`.`encounter_id` = (SELECT `#{db}`.`encounter`.`encounter_id` FROM `#{db}`.`encounter` " + 
        " WHERE `#{db}`.`encounter`.`encounter_id` = `#{db}`.`encounter`.`encounter_id` AND "  + 
        "`#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} AND `#{db}`.`encounter`.`encounter_type` = " + 
        "(SELECT encounter_type_id FROM `#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`name` = 'Give Drugs') LIMIT 1) " + 
        "AND COALESCE((`#{db}`.regimen_category(#{person["patient_id"]}, " + 
        "(SELECT `#{db}`.`drug_order`.`drug_inventory_id` FROM `#{db}`.`drug_order` WHERE `#{db}`.`drug_order`.`order_id` = " + 
        "`#{db}`.`orders`.`order_id` LIMIT 1), DATE(`#{db}`.`orders`.`date_created`))),'') != ''" 
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO obs SELECT NULL, #{person["patient_id"]}, (SELECT concept_id FROM concept_name WHERE name = " + 
          "'Regimen category' LIMIT 1), `#{db}`.`orders`.`encounter_id`, NULL, " + 
          "`#{db}`.`orders`.`date_created`, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, " + 
          "NULL, NULL, NULL, (`#{db}`.regimen_category(#{person["patient_id"]}, " + 
          "(SELECT `#{db}`.`drug`.`concept_id` FROM `#{db}`.`drug` LEFT OUTER JOIN `#{db}`.`drug_order` ON `#{db}`.`drug`.`drug_id` = " + 
          " `#{db}`.`drug_order`.`drug_inventory_id` WHERE `#{db}`.`drug_order`.`order_id` = " + 
          "`#{db}`.`orders`.`order_id` LIMIT 1), DATE(`#{db}`.`orders`.`date_created`))), NULL, NULL, NULL, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "(SELECT creator FROM encounter WHERE encounter_id = " + 
          "`#{db}`.`orders`.`encounter_id`)), `#{db}`.`orders`.`date_created`, NULL, NULL, " + 
          "NULL, NULL, NULL, (SELECT UUID()) FROM `#{db}`.`orders` WHERE " + 
          "`#{db}`.`orders`.`encounter_id` = (SELECT `#{db}`.`encounter`.`encounter_id` FROM `#{db}`.`encounter` " + 
          " WHERE `#{db}`.`encounter`.`encounter_id` = `#{db}`.`encounter`.`encounter_id` AND "  + 
          "`#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} AND `#{db}`.`encounter`.`encounter_type` = " + 
          "(SELECT encounter_type_id FROM `#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`name` = 'Give Drugs') " + 
          " AND `#{db}`.`encounter`.`patient_id` IN (SELECT DISTINCT `#{db}`.`patient_program`.`patient_id` " + 
          " FROM `#{db}`.`patient_program`) LIMIT 1) " + 
          "AND COALESCE((`#{db}`.regimen_category(#{person["patient_id"]}, " + 
          "(SELECT `#{db}`.`drug_order`.`drug_inventory_id` FROM `#{db}`.`drug_order` WHERE `#{db}`.`drug_order`.`order_id` = " + 
          "`#{db}`.`orders`.`order_id` LIMIT 1), DATE(`#{db}`.`orders`.`date_created`))),'') != ''"
      next
      
    end
    
    obs = dest_con.query("SELECT * FROM `obs` WHERE NOT `obs_id` IN (SELECT `obs_edit_audit`.`obs_id` FROM `obs_edit_audit` " + 
        " WHERE `obs_edit_audit`.`obs_id` = `obs`.`obs_id`) AND concept_id = (SELECT concept_id FROM concept_name WHERE name = " + 
        "'Antiretroviral treatment status') AND person_id = #{person["patient_id"]}") 

    obs.each_hash do |ob|
      t = Thread.new {
    
        begin
      
          print "# importing patient_states for patient with id #{ob["person_id"]}\n"
      
          p = dest_con.query("INSERT INTO patient_state SELECT NULL, (SELECT patient_program_id FROM patient_program WHERE " + 
              " patient_id = person_id ORDER BY date_created DESC LIMIT 1), " + 
              "COALESCE((SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = value_coded " + 
              " AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = " + 
              "(SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = " + 
              "'HIV PROGRAM' LIMIT 1)))), NULL), obs_datetime, NULL, creator, obs_datetime, NULL, NULL, NULL, NULL, " + 
              "NULL, NULL, (SELECT UUID()) " + 
              " FROM obs WHERE obs_id = #{ob["obs_id"]}")
      
          p = dest_con.query("INSERT INTO obs_edit_audit (obs_id) VALUES (#{ob["obs_id"]})")
      
        rescue Mysql::Error => e
          puts "?? Error #{e.errno}: #{e.error}"
      
          puts "?? Query: " + "INSERT INTO patient_state SELECT NULL, (SELECT patient_program_id FROM patient_program WHERE " + 
            " patient_id = person_id ORDER BY date_created DESC LIMIT 1), " + 
            "COALESCE((SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = value_coded " + 
            " AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = " + 
            "(SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = " + 
            "'HIV PROGRAM' LIMIT 1)))), NULL), obs_datetime, NULL, creator, obs_datetime, NULL, NULL, NULL, NULL, " + 
            "NULL, NULL, (SELECT UUID()) " + 
            " FROM obs WHERE obs_id = #{ob["obs_id"]}" 
      
          log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO patient_state SELECT NULL, (SELECT patient_program_id FROM patient_program WHERE " + 
              " patient_id = person_id ORDER BY date_created DESC LIMIT 1), " + 
              "COALESCE((SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = value_coded " + 
              " AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = " + 
              "(SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = " + 
              "'HIV PROGRAM' LIMIT 1)))), NULL), obs_datetime, NULL, creator, obs_datetime, NULL, NULL, NULL, NULL, " + 
              "NULL, NULL, (SELECT UUID()) " + 
              " FROM obs WHERE obs_id = #{ob["obs_id"]}"
          next      
      
        end
      }
    end
    
    begin
      print "# importing orders for patient with id #{person["patient_id"]}\n"
      
      p = dest_con.query("INSERT INTO orders SELECT `#{db}`.`orders`.`order_id`, `#{db}`.`orders`.`order_type_id`, " + 
          "`#{db}`.`orders`.`concept_id`,(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`orderer`), `#{db}`.`orders`.`encounter_id`, " + 
          "`#{db}`.`orders`.`instructions`, COALESCE(`#{db}`.`orders`.`start_date`, `#{db}`.`orders`.`date_created`), " + 
          "COALESCE(`#{db}`.`orders`.`auto_expire_date`, (ADDDATE(COALESCE(`#{db}`.`orders`.`start_date`, " + 
          "`#{db}`.`orders`.`date_created`), INTERVAL (SELECT `#{db}`.`drug_order`.`quantity` FROM `#{db}`.`drug_order` " + 
          " WHERE `#{db}`.`drug_order`.`order_id` = `#{db}`.`orders`.`order_id` LIMIT 1)/COALESCE(" + 
          "(SELECT `#{db}`.`patient_prescription_totals`.`daily_consumption` "  + 
          " FROM `#{db}`.`patient_prescription_totals` " + 
          "WHERE `#{db}`.`patient_prescription_totals`.`patient_id` = #{person["patient_id"]} AND " + 
          "`#{db}`.`patient_prescription_totals`.`drug_id` = (SELECT `#{db}`.`drug_order`.`drug_inventory_id` " + 
          " FROM `#{db}`.`drug_order` WHERE `#{db}`.`drug_order`.`order_id` = `#{db}`.`orders`.`order_id` LIMIT 1) AND " + 
          "`#{db}`.`patient_prescription_totals`.`prescription_date` = (SELECT DATE(`#{db}`.`encounter`.`encounter_datetime`) FROM " + 
          "`#{db}`.`encounter` WHERE `#{db}`.`encounter`.`encounter_id` = `#{db}`.`orders`.`encounter_id`)), 1)  DAY))), " + 
          "`#{db}`.`orders`.`discontinued`, `#{db}`.`orders`.`discontinued_date`, (SELECT `users_mapping`.`bart2_user_id` " + 
          "FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`discontinued_by`), " + 
          "`#{db}`.`orders`.`discontinued_reason`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`creator`), `#{db}`.`orders`.`date_created`, " + 
          "`#{db}`.`orders`.`voided`,  (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`voided_by`), `#{db}`.`orders`.`date_voided`, " + 
          "`#{db}`.`orders`.`void_reason`, " + 
          "#{person["patient_id"]}, NULL, NULL, (SELECT UUID()), NULL" + 
          " FROM `#{db}`.`orders` WHERE `#{db}`.`orders`.`encounter_id` IN (SELECT `#{db}`.`orders`.`encounter_id` " + 
          "FROM `#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` =  #{person["patient_id"]} " + 
          " AND `#{db}`.`encounter`.`encounter_id` = `#{db}`.`orders`.`encounter_id`) " + 
          " ON DUPLICATE KEY UPDATE order_id = `#{db}`.`orders`.`order_id`")
      
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " 
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO orders SELECT `#{db}`.`orders`.`order_id`, `#{db}`.`orders`.`order_type_id`, " + 
          "`#{db}`.`orders`.`concept_id`,(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`orderer`), `#{db}`.`orders`.`encounter_id`, " + 
          "`#{db}`.`orders`.`instructions`, COALESCE(`#{db}`.`orders`.`start_date`, `#{db}`.`orders`.`date_created`), " + 
          "COALESCE(`#{db}`.`orders`.`auto_expire_date`, (ADDDATE(COALESCE(`#{db}`.`orders`.`start_date`, " + 
          "`#{db}`.`orders`.`date_created`), INTERVAL (SELECT `#{db}`.`drug_order`.`quantity` FROM `#{db}`.`drug_order` " + 
          " WHERE `#{db}`.`drug_order`.`order_id` = `#{db}`.`orders`.`order_id` LIMIT 1)/COALESCE(" + 
          "(SELECT `#{db}`.`patient_prescription_totals`.`daily_consumption` "  + 
          " FROM `#{db}`.`patient_prescription_totals` " + 
          "WHERE `#{db}`.`patient_prescription_totals`.`patient_id` = #{person["patient_id"]} AND " + 
          "`#{db}`.`patient_prescription_totals`.`drug_id` = (SELECT `#{db}`.`drug_order`.`drug_inventory_id` " + 
          " FROM `#{db}`.`drug_order` WHERE `#{db}`.`drug_order`.`order_id` = `#{db}`.`orders`.`order_id` LIMIT 1) AND " + 
          "`#{db}`.`patient_prescription_totals`.`prescription_date` = (SELECT DATE(`#{db}`.`encounter`.`encounter_datetime`) FROM " + 
          "`#{db}`.`encounter` WHERE `#{db}`.`encounter`.`encounter_id` = `#{db}`.`orders`.`encounter_id`)), 1)  DAY))), " + 
          "`#{db}`.`orders`.`discontinued`, `#{db}`.`orders`.`discontinued_date`, (SELECT `users_mapping`.`bart2_user_id` " + 
          "FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`discontinued_by`), " + 
          "`#{db}`.`orders`.`discontinued_reason`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`creator`), `#{db}`.`orders`.`date_created`, " + 
          "`#{db}`.`orders`.`voided`,  (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`voided_by`), `#{db}`.`orders`.`date_voided`, " + 
          "`#{db}`.`orders`.`void_reason`, " + 
          "#{person["patient_id"]}, NULL, NULL, (SELECT UUID()), NULL" + 
          " FROM `#{db}`.`orders` WHERE `#{db}`.`orders`.`encounter_id` IN (SELECT `#{db}`.`orders`.`encounter_id` " + 
          "FROM `#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` =  #{person["patient_id"]} " + 
          " AND `#{db}`.`encounter`.`encounter_id` = `#{db}`.`orders`.`encounter_id`) " + 
          " ON DUPLICATE KEY UPDATE order_id = `#{db}`.`orders`.`order_id`"
      next
      
    end
    
    begin
      print "# importing drug_orders for patient with id #{person["patient_id"]}\n"
      
      p = dest_con.query("INSERT INTO drug_order SELECT `#{db}`.`drug_order`.`order_id`, " + 
          "(SELECT new_drug_id FROM tmp_drug_stack WHERE drug_id = `#{db}`.`drug_order`.`drug_inventory_id`), " + 
          "`#{db}`.`drug_order`.`dose`, (SELECT `#{db}`.`patient_prescription_totals`.`daily_consumption` "  + 
          " FROM `#{db}`.`patient_prescription_totals` " + 
          "WHERE `#{db}`.`patient_prescription_totals`.`patient_id` = #{person["patient_id"]} AND " + 
          "`#{db}`.`patient_prescription_totals`.`drug_id` = `#{db}`.`drug_order`.`drug_inventory_id` AND " + 
          "`#{db}`.`patient_prescription_totals`.`prescription_date` = (SELECT DATE(`#{db}`.`encounter`.`encounter_datetime`) FROM " + 
          "`#{db}`.`encounter` WHERE `#{db}`.`encounter`.`encounter_id` = (SELECT `#{db}`.`orders`.`encounter_id` FROM " + 
          "`#{db}`.`orders` WHERE `#{db}`.`orders`.`order_id` = `#{db}`.`drug_order`.`order_id`))), `#{db}`.`drug_order`.`units`, `#{db}`.`drug_order`.`frequency`, " + 
          "`#{db}`.`drug_order`.`prn`, `#{db}`.`drug_order`.`complex`, `#{db}`.`drug_order`.`quantity`" + 
          " FROM `#{db}`.`drug_order` WHERE `#{db}`.`drug_order`.`order_id` = (SELECT `#{db}`.`orders`.`order_id` FROM " + 
          " `#{db}`.`orders` WHERE `#{db}`.`orders`.`order_id` = `#{db}`.`drug_order`.`order_id` AND " + 
          "`#{db}`.`orders`.`encounter_id` = (SELECT `#{db}`.`encounter`.`encounter_id` FROM " + 
          "`#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} AND  " + 
          "`#{db}`.`encounter`.`encounter_id` = `#{db}`.`orders`.`encounter_id` LIMIT 1) LIMIT 1) " + 
          " ON DUPLICATE KEY UPDATE order_id = `#{db}`.`drug_order`.`order_id`")            
      
      # p = dest_con.query("INSERT INTO obs_edit_audit (patient_id) VALUES (#{person["patient_id"]})")
      
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " 
      
      log.debug "Error #{e.errno}: #{e.error}.\n Failed patient: #{person["patient_id"]}\nQuery: " + "INSERT INTO drug_order SELECT `#{db}`.`drug_order`.`order_id`, " + 
          "(SELECT new_drug_id FROM tmp_drug_stack WHERE drug_id = `#{db}`.`drug_order`.`drug_inventory_id`), " + 
          "`#{db}`.`drug_order`.`dose`, (SELECT `#{db}`.`patient_prescription_totals`.`daily_consumption` "  + 
          " FROM `#{db}`.`patient_prescription_totals` " + 
          "WHERE `#{db}`.`patient_prescription_totals`.`patient_id` = #{person["patient_id"]} AND " + 
          "`#{db}`.`patient_prescription_totals`.`drug_id` = `#{db}`.`drug_order`.`drug_inventory_id` AND " + 
          "`#{db}`.`patient_prescription_totals`.`prescription_date` = (SELECT DATE(`#{db}`.`encounter`.`encounter_datetime`) FROM " + 
          "`#{db}`.`encounter` WHERE `#{db}`.`encounter`.`encounter_id` = (SELECT `#{db}`.`orders`.`encounter_id` FROM " + 
          "`#{db}`.`orders` WHERE `#{db}`.`orders`.`order_id` = `#{db}`.`drug_order`.`order_id`))), `#{db}`.`drug_order`.`units`, `#{db}`.`drug_order`.`frequency`, " + 
          "`#{db}`.`drug_order`.`prn`, `#{db}`.`drug_order`.`complex`, `#{db}`.`drug_order`.`quantity`" + 
          " FROM `#{db}`.`drug_order` WHERE `#{db}`.`drug_order`.`order_id` = (SELECT `#{db}`.`orders`.`order_id` FROM " + 
          " `#{db}`.`orders` WHERE `#{db}`.`orders`.`order_id` = `#{db}`.`drug_order`.`order_id` AND " + 
          "`#{db}`.`orders`.`encounter_id` = (SELECT `#{db}`.`encounter`.`encounter_id` FROM " + 
          "`#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` = #{person["patient_id"]} AND  " + 
          "`#{db}`.`encounter`.`encounter_id` = `#{db}`.`orders`.`encounter_id` LIMIT 1) LIMIT 1) " + 
          " ON DUPLICATE KEY UPDATE order_id = `#{db}`.`drug_order`.`order_id`"
      next
      
    end
    
  }
  t.join
  
  p = dest_con.query("COMMIT")  

end

p = dest_con.query("SET FOREIGN_KEY_CHECKS=1")
p = dest_con.query("SET UNIQUE_CHECKS=1")
