package com.kappaware.kgen;

import org.apache.kafka.common.utils.Utils;

public class Key extends Header {

	static final String[] firstNames = new String[] { "CHLOÉ", "LOUISE", "CAMILLE", "LÉA", "EMMA", "JADE", "MANON", "ALICE", "INÈS", "LÉNA",
			"ZOÉ", "ANNA", "ROMANE", "LÉANA", "LOLA", "JULIA", "MILA", "CLARA", "EVA", "ROSE", "LANA", "LUCIE", "MARGAUX", "MAËLLE", "AMBRE", "JULIETTE", "LÉONIE",
			"MATHILDE", "MIA", "AGATHE", "LILY", "NINA", "CAPUCINE", "ÉLISE", "LINA", "LOUNA", "MAËLYS", "OLIVIA", "PAULINE", "SARAH", "VICTORIA", "ANAÏS", "CHARLOTTE", "ÉLÉNA", "GIULIA", "JEANNE", "EDEN", "ÉLISA", "ÉLOÏSE", "ELSA",
			
			"HUGO", "LUCAS", "JULES", "GABRIEL", "ARTHUR", "LÉO", "RAPHAËL", "MARTIN", "LOUIS", "ETHAN",
			"MAXIME", "NATHAN", "PAUL", "GABIN", "BAPTISTE", "LIAM", "AXEL", "MAËL", "THÉO", "ROBIN", "SACHA", "TIMÉO", "TOM", "NOLAN", "ANTOINE", "NOÉ", "MALO", "VICTOR", "AARON", "CLÉMENT", "THOMAS",
			"ENZO", "MAXENCE", "VALENTIN", "ALEXIS", "ELIOTT", "MATHIS", "ÉVAN", "SIMON", "ADAM", "ALEXANDRE", "AUGUSTIN", "NOAH", "TIAGO", "ANTONIN", "BENJAMIN", "MATHYS", "LENNY", "ROMAIN", "SAMUEL" 
			
	};

	
	private String recipient;
	
	public Key(ExtTs extTs) {
		super(extTs, null);
		// Arrange to first name being almost random, but dependendant of the counter value.
		this.recipient = firstNames[ Utils.abs(Utils.murmur2(Long.toString(extTs.getCounter()).getBytes())) % firstNames.length ];
		this.setPartitionKey(recipient);
	}
	
	public String getRecipient() {
		return recipient;
	}
	
}