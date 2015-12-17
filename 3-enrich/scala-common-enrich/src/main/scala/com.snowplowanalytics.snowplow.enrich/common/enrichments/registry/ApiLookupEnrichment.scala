/*
 * Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package enrich
package common
package enrichments
package registry

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// Scala
import scala.util.control.NonFatal

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.parseJson
import org.json4s.jackson.JsonMethods.fromJsonNode

// Iglu
import iglu.client.{
  SchemaCriterion,
  SchemaKey
}

// This project
import utils.ScalazJson4sUtils
import outputs.EnrichedEvent
import apilookup._

/**
 * Lets us create an ApiLookupEnrichmentConfig from a JValue.
 */
object ApiLookupEnrichmentConfig extends ParseableEnrichment {

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "api_lookup_enrichment_config", "jsonschema", 1, 0, 0)

  /**
   * Creates an ApiLookupEnrichment instance from a JValue.
   *
   * @param config The enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment
   * @return a configured ApiLookupEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[ApiLookupEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      (for {
        inputs      <- ScalazJson4sUtils.extract[List[Input]](config, "parameters", "inputs")
        httpApi     <- ScalazJson4sUtils.extract[Option[HttpApi]](config, "parameters", "api", "http")
        jsonOutput  <- ScalazJson4sUtils.extract[Option[JsonOutput]](config, "parameters", "output", "json")
        cache       <- ScalazJson4sUtils.extract[Cache](config, "parameters", "cache")
        api         = List(httpApi).flatten.head      // put all further APIs here to pick single one
        output      = List(jsonOutput).flatten.head   // ...and outputs
      } yield ApiLookupEnrichment(inputs, api, output, cache)).toValidationNel
    })
  }
}

case class ApiLookupEnrichment(inputs: List[Input], api: Api, output: Output, cache: Cache) extends Enrichment {
  import ApiLookupEnrichment._

  val version = new DefaultArtifactVersion("0.1.0")

  /**
   * Primary function of the enrichment
   * It will be Failure on failed HTTP request
   * Successful None on skipped lookup (missing key for eg.)
   *
   * @param event currently enriching event
   * @param derivedContexts derived contexts
   * @return none if some inputs were missing, validated JSON context if lookup performed
   */
  def lookup(event: EnrichedEvent,
      derivedContexts: List[JObject],
      customContexts: JsonSchemaPairs,
      unstructEvent: JsonSchemaPairs): ValidationNel[String, Option[JObject]] = {

    val jsonCustomContexts = transformRawPairs(customContexts)
    val jsonUnstructEvent = transformRawPairs(unstructEvent).headOption

    val templateContext = buildTemplateContext(event, derivedContexts, jsonCustomContexts, jsonUnstructEvent)

    templateContext.flatMap { validInputs =>
      validInputs match {
        case Some(inputs) => cache.get(inputs) match {
          case Some(cached) => Some(cached).successNel
          case None => api.perform(inputs, output).toValidationNel
        }
        case None => None.successNel  // Skip. Some input hasn't been found
      }
    }
  }

  /**
   * Get template context out of all inputs
   * If any of inputs missing it will return None
   *
   * @param event current enriching event
   * @param derivedContexts list of contexts derived on enrichment process
   * @return some template context or none if any error (key is missing) occured
   */
  def buildTemplateContext(
      event: EnrichedEvent,
      derivedContexts: List[JObject],
      customContexts: List[JObject],
      unstructEvent: Option[JObject]): ValidationNel[String, Option[Map[String, String]]] = {

    val eventInputs = buildInputsMap(inputs.map(_.getFromEvent(event)))
    val jsonInputs = buildInputsMap(inputs.map(_.getFromJson(derivedContexts, customContexts, unstructEvent)))

    eventInputs |+| jsonInputs
  }
}

/**
 * Companion object containing common methods for Lookups and manipulating data
 */
object ApiLookupEnrichment {

  /**
   * Transform pairs of schema and node obtained from [[utils.shredder.Shredder]]
   * into list of regular self-describing [[JObject]] representing custom context
   * or unstruct event
   *
   * @param pairs list of pairs consisting of schema and Json nodes
   * @return list of regular JObjects
   */
  private def transformRawPairs(pairs: JsonSchemaPairs): List[JObject] = {
    pairs.map { case (schema, node) =>
      val uri = schema.toSchemaUri
      val data = fromJsonNode(node)
      ("schema" -> uri) ~ ("data" -> data)
    }
  }

  /**
   * Build validated optional template context out of list of all inputs
   *
   * @param kvPairs list of validated optional kv pairs derived from POJO and JSON inputs
   * @return validated option template context.
   *         Failure mean input access error (enrichemnt should be failed),
   *         None means some key weren't filled (enrichment should be skipped),
   *         empty Map means this input weren't used (everything successful)
   */
  private def buildInputsMap(
      kvPairs: List[ValidationNel[String, Option[(String, String)]]]
    ): ValidationNel[String, Option[Map[String, String @@ Tags.FirstVal]]] = {
    kvPairs
      .sequenceU                  // Swap List[Validation[F, (K, V)] with Validation[F, List[(K, V)]]
      .map(_.sequence             // Swap List[Option[(K, V)] with Option[List[(K, V)]
      .map(_.toMap                // Build Map[K, V] out of List[(K, V)]
      .mapValues(Tags.FirstVal))) // Pick correct semigroup to not merge values in further
  }

  /**
   * Parse raw response into validated JSON
   *
   * @param response API response assumed to be JSON
   * @return validated JSON
   */
  private[enrichments] def parseResponse(response: String): Validation[String, JValue] =
    try {
      parseJson(response).success
    } catch {
      case NonFatal(e) => e.toString.failure
    }

}
