import { Check } from "lucide-react";
import { cn } from "@/lib/utils";
import type { SagaStep } from "@/lib/api/types";

const STEPS: SagaStep[] = ["RESERVE", "GATEWAY", "SETTLE", "DONE"];

export function SagaStateMachine({ currentStep, compensating }: { currentStep: SagaStep; compensating: boolean }) {
  const currentIndex = STEPS.indexOf(currentStep);

  return (
    <div className="flex items-center">
      {STEPS.map((step, i) => {
        const done = i < currentIndex || currentStep === "DONE";
        const active = i === currentIndex && currentStep !== "DONE";
        return (
          <div key={step} className="flex flex-1 items-center last:flex-none">
            <div className="flex flex-col items-center gap-1.5">
              <div
                className={cn(
                  "flex h-8 w-8 items-center justify-center rounded-full border-2 text-xs font-semibold",
                  done && "border-success bg-success text-success-foreground",
                  active && (compensating ? "border-warning bg-warning text-warning-foreground" : "border-primary bg-primary text-primary-foreground"),
                  !done && !active && "border-border bg-background text-muted-foreground",
                )}
              >
                {done ? <Check className="h-4 w-4" /> : i + 1}
              </div>
              <span className={cn("text-xs", active ? "font-medium text-foreground" : "text-muted-foreground")}>{step}</span>
            </div>
            {i < STEPS.length - 1 && (
              <div className={cn("mx-2 h-0.5 flex-1", i < currentIndex ? "bg-success" : "bg-border")} />
            )}
          </div>
        );
      })}
    </div>
  );
}
