/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package enrich.common
package enrichments
package registry
package apilookup

// Scala
import scala.util.control.NonFatal

// Scalaz
import scalaz._
import Scalaz._

// Json4s
import org.json4s._
import org.json4s.jackson.compactJson

// This project
import outputs.EnrichedEvent


/**
 * Container for key with one (and only one) of possible input sources
 *
 * @param key extracted key
 * @param pojo optional pojo source to take stright from `EnrichedEvent`
 * @param json optional JSON source to take from event context
 */
case class Input(key: String, pojo: Option[PojoInput], json: Option[JsonInput]) {
  // Constructor validation for mapping JSON to `Input` instance
  (pojo, json) match {
    case (None, None)       => { throw new MappingException("Input must represent either json OR pojo, none present") }
    case (Some(_), Some(_)) => { throw new MappingException("Input must represent either json OR pojo, both present") }
    case _ =>
  }

  /**
   * Get key-value pair input from specific `event` for composing
   *
   * @param event currently enriching event
   * @return validated optional tuple of input key and event value
   *         Failure means any non-fatal error on accessing field
   *         successful None means absence (null?) and should be interpreted as enrichment skip
   */
  def getFromEvent(event: EnrichedEvent): ValidationNel[String, Option[(String, String)]] = pojo match {
    case Some(pojoInput) => {
      try {
        val method = event.getClass.getMethod(pojoInput.field)
        val value = Option(method.invoke(event)).map(_.toString)
        value.map(v => (key, v)).successNel
      } catch {
        case NonFatal(err) => s"Error accessing POJO input field [$key]: [$err]".failureNel
      }
    }
    case None => None.successNel
  }

  /**
   * Get value out of list of JSON contexts.
   *
   * @param derived list of self-describing JObjects representing derived contexts
   * @param custom list of self-describing JObjects representing custom contexts
   * @param unstruct optional self-describing JObject representing unstruct event
   * @return optional pair of (key, value) for constructing template context
   *         None means missing value and whole enrichment should be short-circuited
   */
  def getFromJson(derived: List[JObject], custom: List[JObject], unstruct: Option[JObject]): ValidationNel[String, Option[(String, String)]] =
    json match {
      case Some(jsonInput) => {
        val validatedJson: Validation[String, Option[JValue]] = jsonInput.field match {
          case "derived_contexts" => getBySchemaCriterion(derived, jsonInput.schemaCriterion).success
          case "contexts" => getBySchemaCriterion(custom, jsonInput.schemaCriterion).success
          case "unstruct" => getBySchemaCriterion(unstruct.toList, jsonInput.schemaCriterion).success
          case other => s"Error: wrong field [$other] passed to Input.getFromJson. Should be one of: derived_contexts, contexts, unstruct".failure
        }

        validatedJson.flatMap { inputJson =>
          inputJson match {
            case Some(json) => buildKeyValueFromJsonPath(jsonInput.jsonPath, json, key)
            case None => None.success // Skip. Specified context or event isn't presented in Event // TODO: skip?
          }
        }
      }.toValidationNel
      case None => None.successNel
    }

  /**
   * Get data out of all JSON contexts matching `schemaCriterion`
   * If more than one context match schemaCriterion, first will be picked
   *
   * @param contexts list of self-describing JSON contexts attached to event
   * @param schemaCriterion part of URI
   * @return first (optional) self-desc JSON matched `schemaCriterion`
   */
  def getBySchemaCriterion(contexts: List[JObject], schemaCriterion: String): Option[JValue] = {
    val matched = contexts.filter { context =>
      context.obj.exists {
        case ("schema", JString(schema)) => {
          val schemaWithoutProtocol = schema.split(':').drop(1).mkString(":")   // TODO: do we need to drop 'iglu:'?
                                                                                // TODO: use iglu SchemaCriterion?
          schemaWithoutProtocol.startsWith(schemaCriterion)
        }
        case _ => false
      }
    }
    matched.map(_ \ "data").headOption
  }

  /**
   * Get value by JSON Path and convert it to String
   * Absence of value is failure
   *
   * @param jsonPath JSON Path
   * @param context event context as JSON object
   * @return validated optional value
   */
  def getByJsonPath(jsonPath: String, context: JValue): Validation[String, Option[String]] = {
    val result: Validation[String, List[JValue]] = JsonPathExtractor.query(jsonPath, context)
    result.map { list =>
      stringifyJson(JsonPathExtractor.wrapArray(list))
    }
  }

  /**
   * Helper function to stringify JValue to URL-friendly format
   * JValue should be converted to string for further use in URL template with following rules:
   * 1. JString -> as is
   * 2. JInt/JDouble/JBool/null -> stringify
   * 3. JArray -> concatenate with comma ([1,true,"foo"] -> "1,true,foo"). Nested will be flattened
   * 4. JObject -> use as is
   *
   * @param json arbitrary JSON value
   * @return
   */
  def stringifyJson(json: JValue): Option[String] = json match {
    case JString(s) => s.some
    case JArray(array) => array.map(stringifyJson).mkString(",").some
    case obj: JObject => compactJson(obj).some
    case JInt(i) => i.toString.some
    case JDouble(d) => d.toString.some
    case JDecimal(d) => d.toString.some
    case JBool(b) => b.toString.some
    case JNull => "null".some
    case JNothing => None
  }

  /**
   * Proxy function to [[getByJsonPath]] which constructs key-value pair, where value
   * should be fount in `json` by `jsonPath`
   *
   * @param jsonPath JSON Path (probably invalid) as a string
   * @param json context or unstruct event
   * @param key key for constructing template context
   * @return validated optional key-value pair
   */
  private[this] def buildKeyValueFromJsonPath(jsonPath: String, json: JValue, key: String): Validation[String, Option[(String, String)]] =
    getByJsonPath(jsonPath, json).map { optional =>
      optional.map(v => (key, v))
    }
}

/**
 * Describes how to take key from POJO source
 *
 * @param field `EnrichedEvent` object field
 */
case class PojoInput(field: String)

/**
 *
 * @param field where to get this json, one of unstruct_event, contexts or derived_contexts
 * @param schemaCriterion self-describing JSON you are looking for in the given JSON field.
 *                        You can specify only the SchemaVer MODEL (e.g. 1-), MODEL plus REVISION (e.g. 1-1-) etc
 * @param jsonPath JSON Path statement to navigate to the field inside the JSON that you want to use as the input
 */
case class JsonInput(field: String, schemaCriterion: String, jsonPath: String)
