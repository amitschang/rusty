use std::collections::HashMap;

use rand::prelude::*;


struct CardDeck {
    rng: ThreadRng,
    cards: Vec<u8>,
}

impl CardDeck {
    fn new() -> CardDeck {
        CardDeck{rng: thread_rng(), cards: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 10, 10]}
    }
    fn draw(&mut self) -> u8 {
        return *self.cards.choose(&mut self.rng).unwrap();
    }
}

#[derive(Copy, Clone, Eq, Hash, PartialEq, Debug)]
pub struct BlackJackState {
    pub dealer_shown: u8,
    pub player_sum: u8,
    pub usable_ace: bool,
}

pub struct BlackJackEnv {
    deck: CardDeck,
    state: BlackJackState,
    dealer_ace: bool,
}

pub struct Transition {
    pub next_state: BlackJackState,
    pub reward: i8,
    pub terminated: bool,
}

impl BlackJackEnv {
    pub fn new() -> BlackJackEnv {
        let mut env = BlackJackEnv {
            deck: CardDeck::new(),
            state: BlackJackState {dealer_shown: 0, player_sum: 0, usable_ace: false},
            dealer_ace: false,
        };
        env.deal_dealer();
        env.deal_player();
        return env;
    }

    pub fn get_state(&self) -> BlackJackState {
        return self.state;
    }

    fn check_ace(card: u8, sum: u8, ace: bool) -> bool {
        if sum > 10 {
            return false;
        }
        else if card == 1 {
            return true;
        }
        return ace;
    }

    fn deal_dealer(&mut self) {
        let card = self.deck.draw();
        self.state.dealer_shown += card;
        self.dealer_ace = Self::check_ace(card, self.state.dealer_shown, self.dealer_ace);
    }

    fn deal_player(&mut self) {
        let card = self.deck.draw();
        self.state.player_sum += card;
        self.state.usable_ace = Self::check_ace(card, self.state.player_sum, self.state.usable_ace);
    }

    fn final_result(&mut self) -> i8 {
       let mut player_score = self.state.player_sum;
       if self.state.usable_ace {
            player_score += 10;
       }
       loop {
            self.deal_dealer();
            let mut dealer_score = self.state.dealer_shown;
            if self.dealer_ace {
                dealer_score += 10;
            }
            if dealer_score > 21 {
                return 1;
            }
            if dealer_score > 16 {
                if player_score > dealer_score {
                    return 1;
                }
                else if dealer_score > player_score {
                    return -1;
                };
                return 0;
            }
        }
    }

    pub fn action(&mut self, hit: bool) -> Transition {
        let mut reward: i8 = 0;
        let mut terminated = false;
        if hit {
            self.deal_player();
            if self.state.player_sum > 21 {
                reward = -1;
                terminated = true;
            }
        }
        else {
            reward = self.final_result();
            terminated = true;
        }
        return Transition {
            next_state: self.state,
            reward,
            terminated,
        }
    }

}


struct BlackJackAgent {
    q_table: HashMap<BlackJackState, [f32; 2]>,
    epsilon: f32,
    epsilon_decay: f32,
    epsilon_min: f32,
    lr: f32,
    discount: f32,
}

impl BlackJackAgent {
    fn new() -> Self {
        Self {
            q_table: HashMap::new(),
            epsilon: 1.0,
            epsilon_decay: 0.01,
            epsilon_min: 0.1,
            lr: 0.01,
            discount: 0.95
        }
    }

    fn select_action(&self, state: &BlackJackState) -> bool {
        let test: f32 = random();
        if test < self.epsilon {
            return random();
        }
        if self.q_table.contains_key(state) {
            let qs = self.q_table.get(state).unwrap();
            if qs[1] > qs[0] {
                return true;
            }
        }
        return false;
    }

    fn update(&mut self, hit: bool, start: &BlackJackState, tx: &Transition) {
        let current_q = match self.q_table.get(&start) {
            Some(q) => q[hit as usize],
            None => 0.0,
        };
        let future_q = match tx.terminated {
            true => 0.0,
            false => match self.q_table.get(&tx.next_state) {
                Some(q) => {
                    if q[0] > q[1] {
                        q[0]
                    } else {
                        q[1]
                    }
                },
                None => 0.0,
            }
        };
        let newq = current_q +
            self.lr * (tx.reward as f32 + self.discount * future_q - current_q);
        if self.q_table.contains_key(&start) {
            self.q_table.get_mut(&start).unwrap()[hit as usize] = newq;
        }
        else {
            let mut newarr = [0.0, 0.0];
            newarr[hit as usize] = newq;
            self.q_table.insert(*start, newarr);
        }
    }

    fn decay_epsilon(&mut self) {
        let new_epsilon = self.epsilon - self.epsilon_decay;
        self.epsilon = match new_epsilon < self.epsilon_min {
            true => self.epsilon_min,
            false => new_epsilon,
        };
    }

    fn play(&mut self, n_games: i32, train: bool) -> i32 {
        let mut total_reward = 0;
        for _ in 0..n_games {
            let mut env = BlackJackEnv::new();
            loop {
                let state = env.get_state();
                let action = self.select_action(&state);
                let tx = env.action(action);
                if train {
                    self.update(action, &state, &tx);
                }
                if tx.terminated {
                    total_reward += tx.reward as i32;
                    break;
                }
            }
            if train {
                self.decay_epsilon();
            }
        }
        return total_reward;
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_env() {
        let env = BlackJackEnv::new();
        assert_ne!(env.state.dealer_shown, 0);
        assert_ne!(env.state.player_sum, 0);
    }

    #[test]
    fn deal_player() {
        let mut env = BlackJackEnv::new();
        let cur = env.state.player_sum;
        env.deal_player();
        assert!(env.state.player_sum > cur);
    }

    #[test]
    fn test_agent_select() {
        let agent = BlackJackAgent::new();
        let env = BlackJackEnv::new();
        agent.select_action(&env.get_state());
    }

    #[test]
    fn test_agent_update() {
        let start = BlackJackState { dealer_shown: 1, player_sum: 1, usable_ace: false};
        let next = BlackJackState { dealer_shown: 1, player_sum: 1, usable_ace: false};
        let tx = Transition { next_state: next, reward: 1, terminated: false};
        let mut agent = BlackJackAgent::new();
        agent.update(true, &start, &tx);
        assert!(agent.q_table.contains_key(&start));
        agent.update(false, &start, &tx);
        println!("q_table: {:?}", agent.q_table);
    }

    #[test]
    fn test_train() {
        let mut agent = BlackJackAgent::new();
        let pretrain_score = agent.play(100, false);
        let initial_epsilon = agent.epsilon;
        agent.play(1_000, true);
        assert!(initial_epsilon > agent.epsilon);
        let posttrain_score = agent.play(100, false);
        assert!(pretrain_score < posttrain_score);
    }

    #[test]
    fn test_struct_update() {
        let agent = BlackJackAgent {
            lr: 100.0,
            ..BlackJackAgent::new()
        };
        assert_eq!(agent.lr, 100.0);
        assert_eq!(agent.epsilon, 1.0);
    }
}