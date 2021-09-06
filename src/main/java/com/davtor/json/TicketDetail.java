package com.davtor.json;

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
public class TicketDetail {
	
	private long ticketDetailId;
	private long ticketId;
	private String productName;
	private int units;
	private double price;
	
}
