package com.davtor.json;

import com.davtor.avro.Customer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Ticket {
	
	private long ticketId;
	private String customerId;
	private String date;
	private boolean filled;
	private String customerName;
}
