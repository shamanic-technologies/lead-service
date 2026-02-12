import { Router } from "express";

const router = Router();

router.get("/", (req, res) => {
  res.json({ status: "ok", service: "lead-service" });
});

router.get("/health", (req, res) => {
  res.json({ status: "ok", service: "lead-service" });
});

export default router;
