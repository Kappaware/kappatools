package com.kappaware.k2kj.nju;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TestJavaPipelines {
	
	static class Person {
		private String name;
		private Integer age;
		
		public Person(String name, Integer age) {
			super();
			this.name = name;
			this.age = age;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public Integer getAge() {
			return age;
		}
		public void setAge(Integer age) {
			this.age = age;
		}
		
		
	}
	public static void main(String[] argv) {
		
		List<Person> persons = Arrays.asList(new Person[] { new Person("Jules", 18), new Person("Marcel", 102), new Person("Corine", 45)});
		
		System.out.println(persons);
		System.out.println(persons.stream().map(Person::getAge));
		System.out.println(persons.stream().map(Person::getAge).toArray());
		System.out.println(Arrays.toString(persons.stream().map(Person::getAge).toArray()));
		System.out.println(persons.stream().map(Person::getName).reduce("", (a, b) -> a + ", " + b));
		
		System.out.println(persons.stream().map(Person::getName).collect(Collectors.toList()));
		
	}
	

}
