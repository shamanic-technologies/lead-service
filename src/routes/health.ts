import { Router } from "express";

const router = Router();

router.get("/health", (req, res) => {
  res.json({ status: "ok", service: "lead-guard" });
});

export default router;
