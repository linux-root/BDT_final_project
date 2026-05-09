package com.bigdata2026.frontend

import cats.effect.IO
import scala.scalajs.js.annotation.*
import tyrian.*
import tyrian.Html.*

@JSExportTopLevel("TyrianApp")
object Main extends TyrianIOApp[Msg, Model]:
  def router: Location => Msg = Routing.none(Msg.NoOp)

  def init(flags: Map[String, String]): (Model, Cmd[IO, Msg]) =
    (Model(), Cmd.None)

  def update(model: Model): Msg => (Model, Cmd[IO, Msg]) =
    _ => (model, Cmd.None)

  def view(model: Model): Html[Msg] =
    div()(
      h1()(text("BigData2026 — Visualization")),
      p()(text("Frontend skeleton. Wire HBase-backed views via /backend API."))
    )

  def subscriptions(model: Model): Sub[IO, Msg] = Sub.None

final case class Model()

enum Msg:
  case NoOp
