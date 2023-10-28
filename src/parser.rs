use std::{error::Error, collections::HashMap};
use bson::{doc, Document, Bson};
use chrono::NaiveDateTime;
use regex::Regex;

struct QueryData {
    data: Vec<(String, i64)>,
    execution_time: i64,
    plan_summary: String,
    command: String,
    db: String,
    collection: String,
}

#[derive(Eq, PartialEq, Hash)]
enum RegexType {
    Line,
    Infos,
    ExecutionTime,
    PlanSummary,
    CommandWrite,
    CommandRead,
    DbWrite,
    DbRead,
    CollectionWrite,
    CollectionRead
}

pub struct Parser {
    regexs: HashMap<RegexType, Regex>,
}

impl Parser {
    pub fn new() -> Parser {
        let mut regexs: HashMap<RegexType, Regex> = HashMap::new();
        regexs.insert(RegexType::Line, Regex::new(r"^(?P<timestamp>[0-9\-]+?T[0-9\.\:\-]+?)\s+(?P<severity>[A-Z]{1})\s+(?P<component>[A-Z\_]+?)\s+\[(?P<context>.+?)\]\s+(?P<message>.+)$").unwrap());
        regexs.insert(RegexType::Infos, Regex::new(r"(bytesRead|nreturned|docsExamined|keysExamined|reslen):?\s?([0-9]+)").unwrap());
        regexs.insert(RegexType::ExecutionTime, Regex::new(r"([0-9]+)ms$").unwrap());
        regexs.insert(RegexType::PlanSummary, Regex::new(r"planSummary: ([A-Z_]+)").unwrap());
        regexs.insert(RegexType::CommandWrite, Regex::new(r"(warning:.+?)?(update|remove|insert)").unwrap());
        regexs.insert(RegexType::CommandRead, Regex::new(r"(command): ([a-zA-Z_]+)").unwrap());
        regexs.insert(RegexType::DbWrite, Regex::new(r"(update|remove|insert)\s+(.+?)\.").unwrap());
        regexs.insert(RegexType::DbRead, Regex::new(r#"(\$db): "(.+?)""#).unwrap());
        regexs.insert(RegexType::CollectionWrite, Regex::new(r"(update|remove|insert)\s+.+?\.(.+?)\s+").unwrap());
        regexs.insert(RegexType::CollectionRead, Regex::new(r#"(find|aggregate|count|distinct|findAndModify):\s+"(.+?)""#).unwrap());

        Parser { regexs }
    }

    pub fn parse_line(&mut self, log_line: String) -> Result<bson::Document, Box<dyn Error>> {
        let regex = self.regexs.get(&RegexType::Line).unwrap();
        if let Some(captures) = regex.captures(&log_line) {
            let timestamp = captures.name("timestamp").unwrap().as_str();
            let severity = captures.name("severity").unwrap().as_str();
            let component = captures.name("component").unwrap().as_str();
            let message = captures.name("message").unwrap().as_str();
    
            let dt = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H:%M:%S%.3f%z")?;
            let query_data = self.get_metrics_from_string(message, component);
    
            let mut doc = doc! {
                "timestamp": bson::DateTime::from_millis(dt.timestamp_millis()),
                "severity": severity,
                "component": component,
                "message_raw": message,
                "execution_time": query_data.execution_time,
                "plan_summary": query_data.plan_summary,
                "command": query_data.command,
                "db": query_data.db,
                "collection": query_data.collection,
            };
    
            for (key, value) in query_data.data {
                doc.insert(key, value);
            }

            self.remove_empty_string_fields(&mut doc);
            Ok(doc)
            
        } else {
            Err(From::from("No match"))
        }
    }
    
    fn get_metrics_from_string(&mut self, message_raw: &str, component: &str) -> QueryData{
    
  
        let command_regex = if component == "WRITE" {
            self.regexs.get(&RegexType::CommandWrite).unwrap()
        } else {
            self.regexs.get(&RegexType::CommandRead).unwrap()
        };
    
        let db_regex = if component == "WRITE" {
            self.regexs.get(&RegexType::DbWrite).unwrap()
        } else {
            self.regexs.get(&RegexType::DbRead).unwrap()
        };
    
        let collection_regex = if component == "WRITE" {
            self.regexs.get(&RegexType::CollectionWrite).unwrap()
        } else {
            self.regexs.get(&RegexType::CollectionRead).unwrap()
        };
    
        // Extrair dados
        let data: Vec<(String, i64)> = self.regexs.get(&RegexType::Infos).unwrap().captures_iter(&message_raw)
            .map(|caps| (caps[1].to_string(), caps[2].parse().unwrap()))
            .collect();
    
        let execution_time: i64 = self.regexs.get(&RegexType::ExecutionTime).unwrap().captures(&message_raw)
            .map(|caps| caps[1].parse().unwrap())
            .unwrap_or_default();
    
        let plan_summary: String = self.regexs.get(&RegexType::PlanSummary).unwrap().captures(&message_raw)
            .map(|caps| caps[1].to_string())
            .unwrap_or_default();
    
        let command: String = command_regex.captures(&message_raw)
            .map(|caps| caps[2].to_string())
            .unwrap_or_default();
    
        let db: String = db_regex.captures(&message_raw)
            .map(|caps| caps[2].to_string())
            .unwrap_or_default();
    
        let collection: String = collection_regex.captures(&message_raw)
            .map(|caps| caps[2].to_string())
            .unwrap_or_default();
    
        // Agora você pode usar os valores extraídos como desejar
        let query_data = QueryData {
            data,
            execution_time,
            plan_summary,
            command,
            db,
            collection,
        };
    
        query_data
    }

    fn remove_empty_string_fields(&mut self, doc: &mut Document) {
        let keys_to_remove: Vec<String> = doc
            .iter()
            .filter_map(|(key, value)| {
                if let Bson::String(s) = value {
                    if s.is_empty() {
                        Some(key.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
    
        for key in keys_to_remove {
            doc.remove(&key);
        }
    }
}