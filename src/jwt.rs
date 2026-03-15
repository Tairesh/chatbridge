use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: Uuid,
    pub iat: i64,
}

pub fn sign(client_id: Uuid, secret: &[u8]) -> String {
    let claims = Claims {
        sub: client_id,
        iat: chrono::Utc::now().timestamp(),
    };
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .expect("JWT encoding cannot fail")
}

pub fn verify(token: &str, secret: &[u8]) -> Option<Uuid> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.required_spec_claims.clear();
    let data =
        jsonwebtoken::decode::<Claims>(token, &DecodingKey::from_secret(secret), &validation)
            .ok()?;
    Some(data.claims.sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"test-secret-at-least-32-bytes-long!!";

    #[test]
    fn sign_and_verify_roundtrip() {
        let client_id = Uuid::new_v4();
        let token = sign(client_id, SECRET);
        let result = verify(&token, SECRET);
        assert_eq!(result, Some(client_id));
    }

    #[test]
    fn verify_invalid_token_returns_none() {
        assert_eq!(verify("not.a.jwt", SECRET), None);
    }

    #[test]
    fn verify_wrong_secret_returns_none() {
        let client_id = Uuid::new_v4();
        let token = sign(client_id, SECRET);
        let result = verify(&token, b"wrong-secret-that-is-also-32-bytes!!");
        assert_eq!(result, None);
    }

    #[test]
    fn verify_tampered_token_returns_none() {
        let client_id = Uuid::new_v4();
        let mut token = sign(client_id, SECRET);
        // Flip last character of signature
        let last = token.pop().unwrap();
        token.push(if last == 'A' { 'B' } else { 'A' });
        assert_eq!(verify(&token, SECRET), None);
    }
}
