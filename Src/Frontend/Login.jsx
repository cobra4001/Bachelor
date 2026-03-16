import { useState } from "react";

export default function Login() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [message, setMessage] = useState("");

  const handleLogin = async (e) => {
    e.preventDefault();

    const formData = new URLSearchParams();
    formData.append("username", username);
    formData.append("password", password);

    try {
      const response = await fetch("http://localhost:8000/token", {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded"
        },
        body: formData
      });

      const data = await response.json();

      if (response.ok) {
        localStorage.setItem("token", data.access_token);
        setMessage("Innlogging vellykket");
      } else {
        setMessage(data.detail || "Innlogging feilet");
      }
    } catch (error) {
      setMessage("Kunne ikke kontakte serveren");
    }
  };

  return (
    <form onSubmit={handleLogin}>
      <h2>Logg inn</h2>
      <input
        type="text"
        placeholder="Brukernavn"
        value={username}
        onChange={(e) => setUsername(e.target.value)}
      />
      <input
        type="password"
        placeholder="Passord"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
      />
      <button type="submit">Logg inn</button>
      <p>{message}</p>
    </form>
  );
}