package com.se.data;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

@Entity("Demography")
public class DemographyEntry {
	@Id
	private String id;
	private Integer year;
	private String ethnicity;
	private Integer ethnicityCode;
	private String gender;
	private Integer age;
	private String county;
	private Integer countyCode;
	private Long population;

	public DemographyEntry() {

	}

	public void createId() {
		id = county + "_" + year + "_" + ethnicity + "_" + gender + "_" + age;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public String getEthnicity() {
		return ethnicity;
	}

	public void setEthnicity(String ethnicity) {
		this.ethnicity = ethnicity;
	}

	public Integer getEthnicityCode() {
		return ethnicityCode;
	}

	public void setEthnicityCode(Integer ethnicityCode) {
		this.ethnicityCode = ethnicityCode;
	}

	public String getGender() {
		if(gender.equals("male")){
			return "1";
		} else{
			return "2";
		}
//		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public String getCounty() {
		return county;
	}

	// County Code, County Name, Year, Race Code, Race Name
	public void setCounty(String county) {
		this.county = county;
	}

	public Integer getCountyCode() {
		return countyCode;
	}

	public void setCountyCode(Integer countyCode) {
		this.countyCode = countyCode;
	}

	public Long getPopulation() {
		return population;
	}

	public void setPopulation(Long population) {
		this.population = population;
	}

	public boolean filtered(DemographyEntry demographyEntry) {
		if (demographyEntry.age != null) {
			if (!demographyEntry.age.equals(age)) {
				return false;
			}
		}
		if (demographyEntry.year != null) {
			if (!demographyEntry.year.equals(year)) {
				return false;
			}
		}
		if (demographyEntry.ethnicity != null) {
			if (!demographyEntry.ethnicity.equals(ethnicity)) {
				return false;
			}
		}
		if (demographyEntry.ethnicityCode != null) {
			if (!demographyEntry.ethnicityCode.equals(ethnicityCode)) {
				return false;
			}
		}
		if (demographyEntry.county != null) {
			if (!demographyEntry.county.equals(county)) {
				return false;
			}
		}
		if (demographyEntry.countyCode != null) {
			if (!demographyEntry.countyCode.equals(countyCode)) {
				return false;
			}
		}

		return true;
	}

}
