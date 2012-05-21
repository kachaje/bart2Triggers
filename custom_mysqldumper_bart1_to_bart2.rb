#!/usr/bin/env ruby
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

dest_con = Mysql.connect(dest_host, dest_user, dest_pass, dest_db)

sort_weight_tables = ["person", "patient", "patient_identifier", "encounter", "patient_program", 
  "obs", "patient_state", "patient_state2", "orders", "drug_order"]

source_tables = {
  "person"=>["patient_id", "gender", "birthdate", "birthdate_estimated", "dead", "death_date", 
    "cause_of_death", "creator", "date_created", "changed_by", "date_changed", "voided", "voided_by",
    "date_voided", "void_reason", "uuid"], 
  
  "patient_name"=>["patient_name_id", "preferred", "person_id", "prefix", "given_name", "middle_name",
    "family_name_prefix", "family_name", "family_name2", "family_name_suffix", "degree", "creator",
    "date_created", "voided", "voided_by", "date_voided", "void_reason", "changed_by", "date_changed", "uuid"], 
  
  "patient"=>["patient_id", "tribe", "creator", "date_created", "changed_by", "date_changed", 
    "voided", "voided_by", "date_voided", "void_reason"], 
  
  "patient_identifier"=>["patient_identifier_id", "patient_id", "identifier", "identifier_type", "preferred", 
    "location_id", "creator", "date_created", "voided", "voided_by", "date_voided", "void_reason", "uuid"], 
  
  "patient_program"=>["patient_program_id", "patient_id", "program_id", "date_enrolled", "date_completed", "creator",
    "date_created", "changed_by", "date_changed", "voided", "voided_by", "date_voided", "void_reason", "uuid", "location_id"], 
  
  "patient_state"=>["patient_state_id", "patient_program_id", "state", "obs_datetime", "end_date", "creator", 
    "date_created", "changed_by", "date_changed", "voided", "voided_by", "date_voided", "void_reason", "uuid" ], 
  
  "patient_state2"=>["patient_state_id", "patient_program_id", "state", "start_date", "end_date", "creator", 
    "date_created", "changed_by", "date_changed", "voided", "voided_by", "date_voided", "void_reason", "uuid" ],
  
  "encounter"=>["encounter_id", "encounter_type", "patient_id", "provider_id", "location_id", "form_id", 
    "encounter_datetime", "creator", "date_created", "voided", "voided_by", "date_voided", 
    "void_reason", "uuid", "changed_by", "date_changed"], 
  
  "obs"=>["obs_id", "person_id", "concept_id", "encounter_id", "order_id", "obs_datetime", "location_id", 
    "obs_group_id", "accession_number", "value_group_id", "value_boolean", "value_coded", 
    "value_coded_name_id", "value_drug", "value_datetime", "value_numeric", "value_modifier", 
    "value_text", "date_started", "date_stopped", "comments", "creator", "date_created", "voided", 
    "voided_by", "date_voided", "void_reason", "value_complex", "uuid"], 
  
  "orders"=>["order_id", "order_type_id", "concept_id", "orderer", "encounter_id", "instructions", 
    "start_date", "auto_expire_date", "discontinued", "discontinued_date", "discontinued_by", 
    "discontinued_reason", "creator", "date_created", "voided", "voided_by", "date_voided", 
    "void_reason", "patient_id", "accession_number", "obs_id", "uuid", "discontinued_reason_non_coded"], 
  
  "drug_order"=>["order_id", "drug_inventory_id", "dose", "equivalent_daily_dose", "units", "frequency", "prn", "complex", "quantity" ], 
  "relationship"=>["relationship_id", "person_a", "relationship", "person_b", "creator", "date_created", "voided", 
    "voided_by", "date_voided", "void_reason", "uuid"]
}

# "Original table" => ["select table", "condition", "target substitute", "ON DUPLICATE UPDATE primary_key_field"]
display_substitute = {
  "person"=>["patient","","person", "person_id", " ON DUPLICATE KEY UPDATE "],
  
  "patient_name"=> ["patient_name","","person_name"],
  
  "patient"=>["patient","","patient"],
  
  "patient_identifier"=>["patient_identifier","WHERE NOT identifier_type IN (2,3,5,6,8,9,11,12)","patient_identifier"],
  
  "patient_program"=>["patient_program","","patient_program"],
  
  "patient_state"=>["obs","WHERE concept_id = 28","patient_state"],
  
  "patient_state2"=>["drug_order","WHERE drug_order.drug_inventory_id IN (SELECT drug_id FROM drug WHERE " + 
      "concept_id IN (450, 451, 452, 458, 826, 827, 828, 829, 453))", "patient_state"],
  
  "encounter"=>["encounter","","encounter", "encounter_id", " ON DUPLICATE KEY UPDATE "],
  
  "obs"=>["obs","","obs"],
  
  "orders"=>["orders","","orders"],
  
  "drug_order"=>["drug_order","","drug_order", "order_id", " ON DUPLICATE KEY UPDATE "],
  
  "relationship"=>["relationship","","relationship"]
}

puts "-- MySQL dump 10.13  Distrib 5.1.62, for debian-linux-gnu (i686)
--
-- Host: localhost    Database: test_db
-- ------------------------------------------------------
-- Server version	5.1.62-0ubuntu0.11.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
"

sort_weight_tables.each do |table|
  
  fields = source_tables[table]
  
  begin
    ds = con.query("SELECT * FROM #{display_substitute[table][0]} #{display_substitute[table][1]} LIMIT 7000, 60")
  
  rescue Mysql::Error => e
    puts "?? Error #{e.errno}: #{e.error}"
      
    puts ":: Query: " + "SELECT * FROM #{display_substitute[table][0]} #{display_substitute[table][1]}"
  end
  
  insert = "# #{table}\nINSERT INTO #{display_substitute[table][2]} VALUES "
  initinsert = "# #{table}\nINSERT INTO #{display_substitute[table][2]} VALUES "
  r = 0
  
  ds.each_hash do |row|
    next if fields.nil?
    
    primary_key = ""
    
    i = 0
    insert = insert + "("
    fields.each do |element|
       
      if element == "uuid"
        uuid = con.query("SELECT UUID() uuid")
        
        uuid = uuid.fetch_hash
        
        row[element] = "'" + uuid["uuid"] + "'"
      elsif row[element].nil? || row[element].empty?
        row[element] = "NULL"         
      elsif element == "patient_program_id" && table == "patient_state2"        
        begin
          patient_program_id = con.query("SELECT patient_program_id FROM patient_program WHERE " + 
              " patient_id = (SELECT patient_id FROM encounter WHERE encounter.encounter_id = " + 
              "(SELECT encounter_id FROM orders WHERE orders.order_id = (#{row["order_id"]}))) ORDER BY date_created DESC LIMIT 1")
        
          patient_program_id = patient_program_id.fetch_hash
        
          row[element] = "'" + patient_program_id["patient_program_id"] + "'" if !patient_program_id.nil?
        
        rescue Mysql::Error => e
          puts "?? Error #{e.errno}: #{e.error}"
      
          puts ":: Query: " + "SELECT patient_program_id FROM patient_program WHERE " + 
            " patient_id = (SELECT patient_id FROM encounter WHERE encounter.encounter_id = " + 
            "(SELECT encounter_id FROM orders WHERE orders.order_id = (#{row["order_id"]}))) ORDER BY date_created DESC LIMIT 1"
        end
      elsif element == "concept_id" && table == "obs2"        
        begin
          concept_id = con.query("SELECT concept_id FROM concept_name WHERE name = " + 
              "'Regimen category' LIMIT 1")
        
          concept_id = concept_id.fetch_hash
        
          row[element] = "'" + concept_id["concept_id"] + "'" if !concept_id.nil?
        
          regimen = con.query("(SELECT regimen_category(patient_id, " + 
              "(SELECT `#{db}`.`drug`.`concept_id` FROM `#{db}`.`drug` LEFT OUTER JOIN `#{db}`.`drug_order` ON `#{db}`.`drug`.`drug_id` = " + 
              " `#{db}`.`drug_order`.`drug_inventory_id` LEFT OUTER JOIN orders ON orders.order_id = drug_order.order_id " + 
              " WHERE `#{db}`.`drug_order`.`order_id` = `#{db}`.`orders`.`order_id` AND encounter.encounter_id = " + 
              "orders.encounter_id LIMIT 1), DATE(encounter.date_created)) regimen FROM encounter WHERE encounter_id = #{row["encounter_id"]}")
        
          regimen = regimen.fetch_hash
        
          row["value_text"] = "'" + regimen["regimen"] + "'" if !regimen.nil?
        rescue Mysql::Error => e
          puts "?? Error #{e.errno}: #{e.error}"
      
          puts ":: Query: " + "SELECT patient_program_id FROM patient_program WHERE " + 
            " patient_id = #{row["patient_id"]} ORDER BY date_created DESC LIMIT 1"
        end
      elsif element == "patient_program_id" && table == "patient_state"        
        begin
          patient_program_id = con.query("SELECT patient_program_id FROM patient_program WHERE " + 
              " patient_id = #{row["patient_id"]} ORDER BY date_created DESC LIMIT 1")
        
          patient_program_id = patient_program_id.fetch_hash
        
          row[element] = "'" + patient_program_id["patient_program_id"] + "'" if !patient_program_id.nil?
        
        rescue Mysql::Error => e
          puts "?? Error #{e.errno}: #{e.error}"
      
          puts ":: Query: " + "SELECT patient_program_id FROM patient_program WHERE " + 
            " patient_id = #{row["patient_id"]} ORDER BY date_created DESC LIMIT 1"
        end
      elsif element == "state" && table == "patient_state2"        
        begin
          state = dest_con.query("SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = " + 
              "(SELECT concept_id FROM concept_name WHERE name = 'On antiretrovirals' LIMIT 1) " + 
              "AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = " + 
              "(SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = " + 
              "'HIV PROGRAM' LIMIT 1)))")
        
          state = state.fetch_hash
        
          row[element] = "'" + state["program_workflow_state_id"] + "'" if !state.nil?
        
        rescue Mysql::Error => e
          puts "?? Error #{e.errno}: #{e.error}"
      
          puts ":: Query: " + "SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = " + 
            "(SELECT concept_id FROM concept_name WHERE name = 'On antiretrovirals' LIMIT 1) " + 
            "AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = " + 
            "(SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = " + 
            "'HIV PROGRAM' LIMIT 1)))"
        end
      elsif element == "state" && table == "patient_state"        
        begin
          state = dest_con.query("SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = " + 
              "(SELECT new_concept_id FROM tmp_concepts_stack WHERE old_concept_id = #{row["value_coded"]} LIMIT 1) " + 
              "AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = " + 
              "(SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = " + 
              "'HIV PROGRAM' LIMIT 1)))")
        
          state = state.fetch_hash
        
          row[element] = "'" + state["program_workflow_state_id"] + "'" if !state.nil?
        
        rescue Mysql::Error => e
          puts "?? Error #{e.errno}: #{e.error}"
      
          puts ":: Query: " + "SELECT program_workflow_state_id FROM program_workflow_state WHERE concept_id = " + 
            "(SELECT concept_id FROM concept_name WHERE name = 'On antiretrovirals' LIMIT 1) " + 
            "AND program_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE program_id = " + 
            "(SELECT program_id FROM program WHERE concept_id = (SELECT concept_id FROM concept_name WHERE name = " + 
            "'HIV PROGRAM' LIMIT 1)))"
        end
      elsif element == "concept_id" || element == "value_coded" 
        if row[element].to_i > 0
          begin
            concept_id = dest_con.query("SELECT new_concept_id FROM tmp_concepts_stack WHERE old_concept_id = #{row[element]}")
        
            concept_id = concept_id.fetch_hash
        
            row[element] = "'" + concept_id["new_concept_id"] + "'" if !concept_id.nil?
        
          rescue Mysql::Error => e
            puts "?? Error #{e.errno}: #{e.error}"
      
            puts ":: Query: " + "SELECT new_concept_id FROM tmp_concepts_stack WHERE old_concept_id = #{row[element]}"
          end
        end
      elsif element == "encounter_type"
        begin
          encounter_type = dest_con.query("SELECT encounter_type_id FROM encounter_type WHERE name = " + 
              "(SELECT CASE `#{db}`.`encounter_type`.`name` WHEN 'HIV FIRST VISIT' THEN 'HIV CLINIC REGISTRATION' " + 
              "WHEN 'ART VISIT' THEN 'HIV CLINIC CONSULTATION'" +
              "WHEN 'GIVE DRUGS' THEN 'DISPENSING'" +          
              "WHEN 'DATE OF ART INITIATION' THEN 'ART ENROLLMENT'" +
              "WHEN 'HEIGHT/WEIGHT' THEN 'VITALS'" +
              "WHEN 'GENERAL RECEPTION' OR 'BARCODE SCAN' THEN 'REGISTRATION'" + 
              "WHEN 'TB RECEPTION' THEN 'TB CLINIC VISIT'" + 
              "WHEN 'REFFERED' THEN 'IS PATIENT REFERRED?'" +
              "END FROM " + 
              "`#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`encounter_type_id` = #{row[element]})")
        
          encounter_type = encounter_type.fetch_hash
        
          row[element] = "'" + encounter_type["encounter_type_id"] + "'" if !encounter_type.nil?
        
        rescue Mysql::Error => e
          puts "?? Error #{e.errno}: #{e.error}"
      
          puts ":: Query: " + "SELECT encounter_type_id FROM encounter_type WHERE name = " + 
            "(SELECT CASE `#{db}`.`encounter_type`.`name` WHEN 'HIV FIRST VISIT' THEN 'HIV CLINIC REGISTRATION' " + 
            "WHEN 'ART VISIT' THEN 'HIV CLINIC CONSULTATION'" +
            "WHEN 'GIVE DRUGS' THEN 'DISPENSING'" +          
            "WHEN 'DATE OF ART INITIATION' THEN 'ART ENROLLMENT'" +
            "WHEN 'HEIGHT/WEIGHT' THEN 'VITALS'" +
            "WHEN 'GENERAL RECEPTION' OR 'BARCODE SCAN' THEN 'REGISTRATION'" + 
            "WHEN 'TB RECEPTION' THEN 'TB CLINIC VISIT'" + 
            "WHEN 'REFFERED' THEN 'IS PATIENT REFERRED?'" +
            "END FROM " + 
            "`#{db}`.`encounter_type` WHERE `#{db}`.`encounter_type`.`encounter_type_id` = #{row[element]}"
        end
      elsif element == "creator" || element == "changed_by" || element == "voided_by" || element == "retired_by"
        begin
          user_id = dest_con.query("SELECT `users_mapping`.`bart2_user_id` user_id FROM " + 
              "`users_mapping` WHERE `users_mapping`.`bart1_user_id` = #{row[element]}")
        
          user_id = user_id.fetch_hash
        
          row[element] = "'" + user_id["user_id"] + "'" if !user_id.nil?
        
        rescue Mysql::Error => e
          puts "?? Error #{e.errno}: #{e.error}"
      
          puts ":: Query: " + "SELECT `users_mapping`.`bart2_user_id` user_id FROM " + 
            "`users_mapping` WHERE `users_mapping`.`bart1_user_id` = " + 
            "#{row[element]}"
        end   
      elsif element == "order_id" && table == "drug_order"
        primary_key = "'" + row[element] + "'"
        row[element] = "'" + row[element] + "'"
      else
        row[element] = "'" + row[element] + "'"
      end
      
      insert = insert + "#{row[element]}" + (i < fields.length-1 ? ", " : "")
      i = i + 1
    end
    
    
    if r >= 100 
      insert = insert + ( ");\n\n")
      
      puts insert
      
      insert = initinsert
      
      r = 0
    else
      insert = insert +  (")") + (r < ds.num_rows - 1 ? ", " : "")
      r = r + 1
    end
  end
  
  insert = insert + ";\n\n"
  
  if !insert.strip.match(/VALUES\s+\;$/)
    puts insert
  end
end

puts "/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;"