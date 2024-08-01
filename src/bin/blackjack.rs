use std::io;

use rusttest::rl::BlackJackEnv;

fn main() {
    let mut env = BlackJackEnv::new();
    println!("START dealer shown: {}", env.get_state().dealer_shown);
    println!("START player sum: {}", env.get_state().player_sum);
    loop {
        // let hit = env.get_state().player_sum < 17;
        let mut hit_choice = String::new();
        io::stdin().read_line(&mut hit_choice).expect("input failed");
        let mut hit = false;
        if hit_choice == "hit\n" {
            println!("hit me!");
            hit = true;
        } else {
            println!("I'll stay!");
        }
        let transition = env.action(hit);
        println!("action dealer shown: {}", env.get_state().dealer_shown);
        println!("action player sum: {}", env.get_state().player_sum);
        println!("action player ace: {}", env.get_state().usable_ace);
        if transition.terminated {
            println!("finished, reward: {}", transition.reward);
            return;
        }
    }
}