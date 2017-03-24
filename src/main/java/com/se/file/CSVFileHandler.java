package com.se.file;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.se.data.DemographyEntry;
import com.se.db.DatabaseUtil;

public class CSVFileHandler {
	String csvFile = "/home/magic/Downloads/CA_DRU_proj_2010-2060.csv";
	String line = "";
	String cvsSplitBy = ",";
	DatabaseUtil databaseUtil = new DatabaseUtil();

	public void readLine() {

		try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
			while ((line = br.readLine()) != null) {
				DemographyEntry demographyEntry = new DemographyEntry();
				String[] csvs = line.split(cvsSplitBy);
				try {
					demographyEntry.setCountyCode(Integer.parseInt(csvs[0]));
					demographyEntry.setCounty(csvs[1]);
					demographyEntry.setYear(Integer.parseInt(csvs[2]));
					demographyEntry.setEthnicityCode(Integer.parseInt(csvs[3]));
					if(csvs.length == 9){
						demographyEntry.setEthnicity(csvs[4] + ',' + csvs[5]);
						demographyEntry.setGender(csvs[6]);
						demographyEntry.setAge(Integer.parseInt(csvs[7]));
						demographyEntry.setPopulation(Long.parseLong(csvs[8]));						
					}
					if(csvs.length == 8){
						demographyEntry.setEthnicity(csvs[4]);
						demographyEntry.setGender(csvs[5]);
						demographyEntry.setAge(Integer.parseInt(csvs[6]));
						demographyEntry.setPopulation(Long.parseLong(csvs[7]));						
					}

					databaseUtil.insert(demographyEntry);
				} catch (NumberFormatException exception) {
					System.out.println("Error");
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public static DemographyEntry read(String value){
		DemographyEntry demographyEntry = new DemographyEntry();
		String[] csvs = value.split(",");
		try {
			demographyEntry.setCountyCode(Integer.parseInt(csvs[0]));
			demographyEntry.setCounty(csvs[1]);
			demographyEntry.setYear(Integer.parseInt(csvs[2]));
			demographyEntry.setEthnicityCode(Integer.parseInt(csvs[3]));
			if(csvs.length == 9){
				demographyEntry.setEthnicity(csvs[4] + ',' + csvs[5]);
				demographyEntry.setGender(csvs[6]);
				demographyEntry.setAge(Integer.parseInt(csvs[7]));
				demographyEntry.setPopulation(Long.parseLong(csvs[8]));						
			}
			if(csvs.length == 8){
				demographyEntry.setEthnicity(csvs[4]);
				demographyEntry.setGender(csvs[5]);
				demographyEntry.setAge(Integer.parseInt(csvs[6]));
				demographyEntry.setPopulation(Long.parseLong(csvs[7]));						
			}
		} catch (NumberFormatException exception) {
			System.out.println("Error");
			return null;
		}
		return demographyEntry;
	}

	public static void main(String[] args) {
		CSVFileHandler csvFileHandler = new CSVFileHandler();
		csvFileHandler.readLine();
	}
}
