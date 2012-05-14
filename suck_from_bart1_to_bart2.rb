#!/usr/bin/ruby -w
require "mysql"
require "yaml"

host = YAML::load_file('database.yml')['bart1_migration_source']['host']
user = YAML::load_file('database.yml')['bart1_migration_source']['username']
pass = YAML::load_file('database.yml')['bart1_migration_source']['password']
db = YAML::load_file('database.yml')['bart1_migration_source']['database']

dest_host = YAML::load_file('database.yml')['bart2_migration_destination']['host']
dest_user = YAML::load_file('database.yml')['bart2_migration_destination']['username']
dest_pass = YAML::load_file('database.yml')['bart2_migration_destination']['password']
dest_db = YAML::load_file('database.yml')['bart2_migration_destination']['database']

con = Mysql.connect(host, user, pass, db)

`mysql --user=#{dest_user} --password=#{dest_pass} -e DROP DATABASE #{dest_db};`

sleep(5)

`mysql --user=#{dest_user} --password=#{dest_pass} -e CREATE DATABASE #{dest_db};`

print "# loading schema/schema.sql file\n"
`mysql --host=#{dest_host} --user=#{dest_user} --password=#{dest_pass} #{dest_db} < schema/schema.sql`

print "# loading schema/patient_report.sql file\n"
`mysql --host=#{dest_host} --user=#{dest_user} --password=#{dest_pass} #{dest_db} < schema/patient_report.sql`

print "# loading schema/patient_report_details.sql file\n"
`mysql --host=#{dest_host} --user=#{dest_user} --password=#{dest_pass} #{dest_db} < schema/patient_report_details.sql`

print "# loading schema/defaults.sql file\n"
`mysql --host=#{dest_host} --user=#{dest_user} --password=#{dest_pass} #{dest_db} < schema/defaults.sql`

triggers = Dir.glob("schema/triggers/*")

triggers.each do |file|  
  print "# loading #{file} trigger file\n"
  `mysql --host=#{dest_host} --user=#{dest_user} --password=#{dest_pass} #{dest_db} < #{file}`
end

dest_con = Mysql.connect(dest_host, dest_user, dest_pass, dest_db)

patients = con.query("SELECT (MAX(patient_id) + 1) lastid FROM patient")

patients = patients.fetch_hash

lastid = patients["lastid"].to_i

begin
  p = dest_con.query("DROP TABLE IF EXISTS `users_mapping`")
  p = dest_con.query("CREATE TABLE `users_mapping` (" + 
      "`bart1_user_id` INT, `bart2_user_id` INT, `bart1_patient_id` INT, `bart2_person_id` INT ) ENGINE = InnoDB")
  
  p = dest_con.query("DELETE FROM relationship")
rescue Mysql::Error => e
  puts "?? Error #{e.errno}: #{e.error}\n"
end
  
users = con.query("SELECT user_id FROM users") 

p = dest_con.query("SET @@FOREIGN_KEY_CHECKS=0")  

users.each_hash do |user|
  print "# importing user with id #{user["user_id"]} to #{lastid}\n"
  
  begin
    p = dest_con.query("INSERT INTO person SELECT #{lastid}, NULL, NULL, NULL, NULL, NULL, " + 
        "NULL, `#{db}`.`users`.`creator`, `#{db}`.`users`.`date_created`, `#{db}`.`users`.`changed_by`, " + 
        "`#{db}`.`users`.`date_changed`, `#{db}`.`users`.`voided`, `#{db}`.`users`.`voided_by`, `#{db}`.`users`.`date_voided`, " + 
        "`#{db}`.`users`.`void_reason`, (SELECT UUID()) FROM `stgabriel`.`users` WHERE `#{db}`.`users`.`user_id` = #{user["user_id"]}")
    
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
        "(SELECT UUID()) FROM `stgabriel`.`users` WHERE `#{db}`.`users`.`user_id` = (SELECT bart1_user_id FROM users_mapping WHERE " + 
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

people = con.query("SELECT patient_id FROM patient") 

people.each_hash do |person|
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
          "WHERE `#{db}`.`patient`.`patient_id` = #{person["patient_id"]}")  

      p = dest_con.query("INSERT INTO patient SELECT `#{db}`.`patient`.`patient_id`, `#{db}`.`patient`.`tribe`, " +
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`creator`), `#{db}`.`patient`.`date_created`, "  + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`changed_by`), `#{db}`.`patient`.`date_changed`, `#{db}`.`patient`.`voided`, "  + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
          "`#{db}`.`patient`.`voided_by`), `#{db}`.`patient`.`date_voided`, `#{db}`.`patient`.`void_reason` " + 
          "FROM `#{db}`.`patient` WHERE `#{db}`.`patient`.`patient_id` = #{person["patient_id"]}")  
      
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
          "#{person["patient_id"]} LIMIT 1)")
      
      p = dest_con.query("INSERT INTO person_address SELECT `#{db}`.`patient_address`.`patient_address_id`, " + 
          "#{person["patient_id"]}, `#{db}`.`patient_address`.`preferred`, `#{db}`.`patient_address`.`address1`, " + 
          "`#{db}`.`patient_address`.`address2`, `city_village`, `#{db}`.`patient_address`.`state_province`, " + 
          "`#{db}`.`patient_address`.`postal_code`, `#{db}`.`patient_address`.`country`, `#{db}`.`patient_address`.`latitude`, " + 
          "`#{db}`.`patient_address`.`longitude`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` " + 
          "WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`patient_address`.`creator`), `#{db}`.`patient_address`.`date_created`, " + 
          "`#{db}`.`patient_address`.`voided`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`patient_address`.`voided_by`), `#{db}`.`patient_address`.`date_voided`, " + 
          "`#{db}`.`patient_address`.`void_reason`, NULL, NULL, NULL, NULL, NULL, (SELECT UUID()) FROM `#{db}`.`patient_address` " + 
          " WHERE `#{db}`.`patient_address`.`patient_id` = #{person["patient_id"]}")
      
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
    
      p = dest_con.query("INSERT INTO encounter SELECT `#{db}`.`encounter`.`encounter_id`, " + 
          "COALESCE((SELECT encounter_type_id FROM encounter_type WHERE name = " + 
          "(SELECT CASE `#{db}`.`encounter_type`.`name` WHEN 'HIV FIRST VISIT' THEN 'HIV CLINIC REGISTRATION' " + 
          "WHEN 'ART VISIT' THEN 'HIV CLINIC CONSULTATION'" +
          "WHEN 'GIVE DRUGS' THEN 'TREATMENT'" +          
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
          "FROM `#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` = #{person["patient_id"]}")
      
      p = dest_con.query("INSERT INTO obs SELECT `#{db}`.`obs`.`obs_id`, #{person["patient_id"]}, `#{db}`.`obs`.`concept_id`, " + 
          "`#{db}`.`obs`.`encounter_id`, `#{db}`.`obs`.`order_id`, `#{db}`.`obs`.`obs_datetime`, `#{db}`.`obs`.`location_id`, " + 
          "`#{db}`.`obs`.`obs_group_id`, `#{db}`.`obs`.`accession_number`, `#{db}`.`obs`.`value_group_id`, " + 
          "`#{db}`.`obs`.`value_boolean`, `#{db}`.`obs`.`value_coded`, NULL, `#{db}`.`obs`.`value_drug`, " + 
          "`#{db}`.`obs`.`value_datetime`, `#{db}`.`obs`.`value_numeric`, `#{db}`.`obs`.`value_modifier`, " + 
          "`#{db}`.`obs`.`value_text`, `#{db}`.`obs`.`date_started`, `#{db}`.`obs`.`date_stopped`, `#{db}`.`obs`.`comments`, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`obs`.`creator`), `#{db}`.`obs`.`date_created`, `#{db}`.`obs`.`voided`, " + 
          "(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`obs`.`voided_by`), `#{db}`.`obs`.`date_voided`, `#{db}`.`obs`.`void_reason`, " + 
          "NULL, (SELECT UUID()) " + 
          " FROM `#{db}`.`obs` WHERE `#{db}`.`obs`.`patient_id` = #{person["patient_id"]}")
      
      p = dest_con.query("INSERT INTO orders SELECT `#{db}`.`orders`.`order_id`, `#{db}`.`orders`.`order_type_id`, " + 
          "`#{db}`.`orders`.`concept_id`,(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`orderer`), `#{db}`.`orders`.`encounter_id`, " + 
          "`#{db}`.`orders`.`instructions`, `#{db}`.`orders`.`start_date`, `#{db}`.`orders`.`auto_expire_date`, " + 
          "`#{db}`.`orders`.`discontinued`, `#{db}`.`orders`.`discontinued_date`, (SELECT `users_mapping`.`bart2_user_id` " + 
          "FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`discontinued_by`), " + 
          "`#{db}`.`orders`.`discontinued_reason`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`creator`), `#{db}`.`orders`.`date_created`, " + 
          "`#{db}`.`orders`.`voided`,  (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`voided_by`), `#{db}`.`orders`.`date_voided`, " + 
          "`#{db}`.`orders`.`void_reason`, " + 
          "#{person["patient_id"]}, NULL, NULL, (SELECT UUID()), NULL" + 
          " FROM `#{db}`.`orders` WHERE `#{db}`.`orders`.`encounter_id` IN (SELECT `#{db}`.`orders`.`encounter_id` " + 
          "FROM `#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` =  #{person["patient_id"]})")
      
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
      
      puts ":: Query: " + "INSERT INTO orders SELECT `#{db}`.`orders`.`order_id`, `#{db}`.`orders`.`order_type_id`, " + 
          "`#{db}`.`orders`.`concept_id`,(SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`orderer`), `#{db}`.`orders`.`encounter_id`, " + 
          "`#{db}`.`orders`.`instructions`, `#{db}`.`orders`.`start_date`, `#{db}`.`orders`.`auto_expire_date`, " + 
          "`#{db}`.`orders`.`discontinued`, `#{db}`.`orders`.`discontinued_date`, (SELECT `users_mapping`.`bart2_user_id` " + 
          "FROM `users_mapping` WHERE `users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`discontinued_by`), " + 
          "`#{db}`.`orders`.`discontinued_reason`, (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`creator`), `#{db}`.`orders`.`date_created`, " + 
          "`#{db}`.`orders`.`voided`,  (SELECT `users_mapping`.`bart2_user_id` FROM `users_mapping` WHERE " + 
          "`users_mapping`.`bart1_user_id` = `#{db}`.`orders`.`voided_by`), `#{db}`.`orders`.`void_reason`, " + 
          "#{person["patient_id"]}, NULL, NULL, (SELECT UUID()), NULL" + 
          " FROM `#{db}`.`orders` WHERE `#{db}`.`orders`.`encounter_id` IN (SELECT `#{db}`.`orders`.`encounter_id` " + 
          "FROM `#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` = #{person["patient_id"]})"
    end
  }
  t.join
end

p = dest_con.query("SET @@FOREIGN_KEY_CHECKS=1")

=begin

people = con.query("SELECT person_id FROM person") 

people.each_hash do |person|
  t = Thread.new {
    # Person table and associated fields
    print "# importing person with id #{person["person_id"]}\n"
      
    begin
      p = dest_con.query("INSERT INTO person SELECT * FROM `#{db}`.`person` " + 
          "WHERE `#{db}`.`person`.`person_id` = #{person["person_id"]}")   
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
    
    print "# importing person_address with id #{person["person_id"]}\n"
      
    begin
      p = dest_con.query("INSERT INTO person_address SELECT * FROM `#{db}`.`person_address` " + 
          "WHERE `#{db}`.`person_address`.`person_id` = #{person["person_id"]}")    
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
    
    print "# importing person_attribute with id #{person["person_id"]}\n"
      
    begin
      p = dest_con.query("INSERT INTO person_attribute SELECT * FROM `#{db}`.`person_attribute` " + 
          "WHERE `#{db}`.`person_attribute`.`person_id` = #{person["person_id"]}")   
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
    
    print "# importing person_name with id #{person["person_id"]}\n"
      
    begin
      p = dest_con.query("INSERT INTO person_name SELECT * FROM `#{db}`.`person_name` " + 
          "WHERE `#{db}`.`person_name`.`person_id` = #{person["person_id"]}")    
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
    
    print "# importing person_name_code with id #{person["person_id"]}\n"
    
    begin
      p = dest_con.query("INSERT INTO person_name_code SELECT * FROM `#{db}`.`person_name_code` " + 
          "WHERE `#{db}`.`person_name_code`.`person_name_id` IN (SELECT person_name_id FROM " + 
          "`#{db}`.`person_name` WHERE `#{db}`.`person_name`.`person_id` = #{person["person_id"]})")    
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
    
    # Patient and associated fields
    print "# importing patient with id #{person["person_id"]}\n"
      
    begin
      p = dest_con.query("INSERT INTO patient SELECT * FROM `#{db}`.`patient` " + 
          "WHERE `#{db}`.`patient`.`patient_id` = #{person["person_id"]}")   
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
    
    print "# importing patient_identifier with id #{person["person_id"]}\n"
      
    begin
      p = dest_con.query("INSERT INTO patient_identifier SELECT * FROM `#{db}`.`patient_identifier` " + 
          "WHERE `#{db}`.`patient_identifier`.`patient_id` = #{person["person_id"]}")    
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
    
    print "# importing patient_program with id #{person["person_id"]}\n"
      
    begin
      p = dest_con.query("INSERT INTO patient_program SELECT * FROM `#{db}`.`patient_program` " + 
          "WHERE `#{db}`.`patient_program`.`patient_id` = #{person["person_id"]}")    
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
    
    print "# importing patient_state with id #{person["person_id"]}\n"
      
    begin
      p = dest_con.query("INSERT INTO patient_state SELECT * FROM `#{db}`.`patient_state` " + 
          "WHERE `#{db}`.`patient_state`.`patient_program_id` IN (SELECT patient_program_id " + 
          "FROM `#{db}`.`patient_program` WHERE `#{db}`.`patient_program`.`patient_id` = #{person["person_id"]})")    
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
    
    # Encounter table     
    print "# importing encounter with patient id #{person["person_id"]}\n"
      
    begin
      p = dest_con.query("INSERT INTO encounter SELECT * FROM `#{db}`.`encounter` " + 
          "WHERE `#{db}`.`encounter`.`patient_id` = #{person["person_id"]}")     
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
    
    # Observations - simple mapping assumed at this stage
    print "# importing observations with patient id #{person["person_id"]}\n"
      
    begin
      p = dest_con.query("SET @@FOREIGN_KEY_CHECKS=0")  
      p = dest_con.query("INSERT INTO obs SELECT * FROM `#{db}`.`obs` " + 
          "WHERE `#{db}`.`obs`.`person_id` = #{person["person_id"]}")    
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
    
    # Orders - simple mapping assumed at this stage
    print "# importing orders with patient id #{person["person_id"]}\n"
      
    begin
      p = dest_con.query("INSERT INTO orders SELECT * FROM `#{db}`.`orders` " + 
          "WHERE `#{db}`.`orders`.`encounter_id` IN (SELECT encounter_id FROM " + 
          "`#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` = #{person["person_id"]})")    
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
     
    print "# importing drug_orders with patient id #{person["person_id"]}\n"
      
    begin
      p = dest_con.query("INSERT INTO drug_order SELECT * FROM `#{db}`.`drug_order` " + 
          "WHERE `#{db}`.`drug_order`.`order_id` IN (SELECT order_id FROM `#{db}`.`orders` " + 
          "WHERE encounter_id IN (SELECT encounter_id FROM " + 
          "`#{db}`.`encounter` WHERE `#{db}`.`encounter`.`patient_id` = #{person["person_id"]}))")    
    rescue Mysql::Error => e
      puts "?? Error #{e.errno}: #{e.error}"
    end
     
    p = dest_con.query("SET @@FOREIGN_KEY_CHECKS=1")
  }
  t.join
end

=end