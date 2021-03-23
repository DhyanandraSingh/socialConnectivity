package com.dhyan.model

import java.time.LocalDateTime

case class User(id: String, name: String, age: Integer, email:String, language: List[String], country: String, joined_at: LocalDateTime )

case class Relationship(id: String, followerId: String, followedId: String, followedAt: LocalDateTime )