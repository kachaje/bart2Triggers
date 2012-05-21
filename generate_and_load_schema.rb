#!/usr/bin/env ruby
require "mysql"
require "yaml"

host = YAML::load_file('database.yml')['migration_source']['host']
user = YAML::load_file('database.yml')['migration_source']['username']
pass = YAML::load_file('database.yml')['migration_source']['password']
db = YAML::load_file('database.yml')['migration_source']['database']

dest_host = YAML::load_file('database.yml')['migration_destination']['host']
dest_user = YAML::load_file('database.yml')['migration_destination']['username']
dest_pass = YAML::load_file('database.yml')['migration_destination']['password']
dest_db = YAML::load_file('database.yml')['migration_destination']['database']

con = Mysql.connect(host, user, pass, db)

rs = con.query("show tables") 
tables = [] 

rs.each_hash{|h| tables << h["Tables_in_" + db]}

exception_tables = ["person", "person_address", "person_attribute", "person_name", 
  "person_name_code", "patient", "patient_identifier", "patient_program", "patient_state",
  "patient_state_on_arvs", "encounter", "obs", "orders", "drug_order", 
  "regimen_observation, relationship"]

tables = tables - exception_tables

`mysql --user=#{dest_user} --password=#{dest_pass} -e DROP DATABASE #{dest_db};`
`mysql --user=#{dest_user} --password=#{dest_pass} -e CREATE DATABASE #{dest_db};`

command = "mysqldump --user=#{user} --password=#{pass} #{db} "

tables.each{|table|
  command += " " + table
}

command += " > schema/defaults.sql\n"

print "# dumping schema/defaults.sql file\n"
`#{command}`

print "# dumping schema/schema.sql file\n"
`mysqldump --user=#{user} --password=#{pass} #{db} --no-data > schema/schema.sql`

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

people = con.query("SELECT person_id FROM person") 

p = dest_con.query("START TRANSACTION")  

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

p = dest_con.query("COMMIT")  