package com.craftcodehouse.promotions.accumulator

case class CustomerEventJoin(customerID: String, email: String, firstName: String,
                             game: String, action: String, stake: Int)
