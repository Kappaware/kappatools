package com.kappaware.kcons;

import java.io.IOException;
import java.io.PrintWriter;

import jline.console.ConsoleReader;

public class TestJline {

	public static void main(String[] argv) throws IOException {
		ConsoleReader reader = new ConsoleReader();
		reader.setPrompt("Test>");

		String line;
		PrintWriter out = new PrintWriter(reader.getOutput());
		while ((line = reader.readLine()) != null) {
			out.printf("==============>'%s'\n", line);
		}

	}

}
