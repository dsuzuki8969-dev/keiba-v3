import { useAuthMode } from "@/api/hooks";

export function useAuth() {
  const { data } = useAuthMode();
  return { isAdmin: data?.admin ?? true };
}
