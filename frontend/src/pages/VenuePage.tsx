import { useParams } from "react-router-dom";
import { VenueListView } from "./VenuePage/VenueListView";
import { VenueDetailView } from "./VenuePage/VenueDetailView";

export default function VenuePage() {
  const { code } = useParams<{ code: string }>();
  return code ? <VenueDetailView code={code} /> : <VenueListView />;
}
