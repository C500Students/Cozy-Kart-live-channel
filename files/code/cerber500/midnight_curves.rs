// Cerber500 Circuit Curve Mapping Engine
// Developed for Termux (Rust) and WebAssembly (Firefox Focus on Android)
// Targets local-first single-file execution and compact screen layouts.

use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrackCurve {
    pub id: u8,
    pub name: String,
    pub technical_rating: char,
    pub difficulty: String,
    pub speed_rating_kph: u32,
    pub description: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CircuitMap {
    pub circuit_name: String,
    pub curves: BTreeMap<u8, TrackCurve>,
    pub lap_record_holder: String,
    pub lap_time_seconds: f32,
}

impl CircuitMap {
    pub fn new() -> Self {
        let mut curves = BTreeMap::new();
        
        curves.insert(1, TrackCurve {
            id: 1,
            name: "The Cat's Paw".to_string(),
            technical_rating: 'A',
            difficulty: "High".to_string(),
            speed_rating_kph: 280,
            description: "High-speed sweeping entry into the neon s-curve complex.".to_string(),
        });
        
        curves.insert(2, TrackCurve {
            id: 2,
            name: "Neon Dip".to_string(),
            technical_rating: 'B',
            difficulty: "Medium".to_string(),
            speed_rating_kph: 245,
            description: "Compressed apex descending toward the central tower hub.".to_string(),
        });

        curves.insert(3, TrackCurve {
            id: 3,
            name: "S-Curve Complex".to_string(),
            technical_rating: 'A',
            difficulty: "High".to_string(),
            speed_rating_kph: 260,
            description: "Rapid weight-transfer chicane requiring precise steering linearity.".to_string(),
        });

        curves.insert(4, TrackCurve {
            id: 4,
            name: "Shadow Spine".to_string(),
            technical_rating: 'C',
            difficulty: "Medium".to_string(),
            speed_rating_kph: 290,
            description: "Sweeping elevated straight passing beneath the MinikoMew display arrays.".to_string(),
        });

        curves.insert(5, TrackCurve {
            id: 5,
            name: "Grandstand Hairpin GP".to_string(),
            technical_rating: 'A',
            difficulty: "High".to_string(),
            speed_rating_kph: 140,
            description: "Heavy braking zone looping back past the illuminated grandstands.".to_string(),
        });

        Self {
            circuit_name: "Cerber500 Midnight City GP".to_string(),
            curves,
            lap_record_holder: "MINIKOMEW".to_string(),
            lap_time_seconds: 74.569,
        }
    }

    pub fn render_termux_cli(&self) {
        println!("==================================================");
        println!("  {} ", self.circuit_name);
        println!("==================================================");
        println!(" Lap Record: {:.3}s by {}", self.lap_time_seconds, self.lap_record_holder);
        println!("--------------------------------------------------");
        for (id, curve) in &self.curves {
            println!("[Corner {}] {}", id, curve.name);
            println!("  > Rating: {} | Difficulty: {} | Max Speed: {} KPH", 
                curve.technical_rating, curve.difficulty, curve.speed_rating_kph);
            println!("  > Info: {}", curve.description);
            println!("--------------------------------------------------");
        }
    }
}

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn get_circuit_json() -> String {
    let map = CircuitMap::new();
    serde_json::to_string(&map).unwrap()
}

fn main() {
    let circuit = CircuitMap::new();
    circuit.render_termux_cli();
}
